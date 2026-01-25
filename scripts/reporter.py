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
        Yahoo FinanceへのリンクURLを生成
        
        Args:
            ticker: 銘柄コード
            
        Returns:
            URL文字列
        """
        # コード整形：.Tを除去し、.0$を正規表現で除去してから4桁に整形
        ticker_clean = str(ticker).replace('.T', '').replace('T', '').strip()
        # re.sub()で.0$を除去してからzfill(4)で4桁に整形
        ticker_clean = re.sub(r'\.0$', '', ticker_clean).strip()
        ticker_clean = ticker_clean.zfill(4)
        url = f"https://finance.yahoo.co.jp/quote/{ticker_clean}.T"
        return url
    
    def _get_yahoo_finance_button(self, ticker: str) -> str:
        """
        Yahoo FinanceへのリンクをBootstrapボタン形式で生成
        
        Args:
            ticker: 銘柄コード
            
        Returns:
            HTMLボタン形式の文字列
        """
        url = self._get_yahoo_finance_link(ticker)
        return f'<a href="{url}" target="_blank" class="btn btn-outline-primary btn-sm">📈 チャート</a>'
    
    def _get_score_stars(self, score: float, max_score: float) -> str:
        """
        スコアを星（★）で視覚化（5段階）
        
        Args:
            score: 現在のスコア
            max_score: 満点
            
        Returns:
            星のHTML文字列
        """
        if max_score == 0:
            return '<span class="text-muted">-</span>'
        
        percentage = (score / max_score) * 100
        filled_stars = int(percentage / 20)  # 20%ごとに1つ星
        half_star = 1 if (percentage % 20) >= 10 else 0
        
        stars_html = '★' * filled_stars
        if half_star and filled_stars < 5:
            stars_html += '☆'
        stars_html += '☆' * (5 - filled_stars - half_star)
        
        return f'<span title="{score:.1f}/{max_score:.0f}点 ({percentage:.0f}%)">{stars_html}</span>'
    
    def _get_score_progress_bar(self, score: float, max_score: float, label: str) -> str:
        """
        スコアをBootstrapのprogress-barで視覚化
        
        Args:
            score: 現在のスコア
            max_score: 満点
            label: ラベル名
            
        Returns:
            progress-barのHTML文字列
        """
        if max_score == 0:
            return f'<small class="text-muted">{label}: N/A</small>'
        
        percentage = min(100, max(0, (score / max_score) * 100))
        
        # 色を決定
        if percentage >= 80:
            bg_class = "bg-success"
        elif percentage >= 60:
            bg_class = "bg-info"
        elif percentage >= 40:
            bg_class = "bg-warning"
        else:
            bg_class = "bg-danger"
        
        return f'''<div class="mb-1">
            <small><strong>{label}:</strong> {score:.1f}/{max_score:.0f}点</small>
            <div class="progress" style="height: 8px;">
                <div class="progress-bar {bg_class}" role="progressbar" style="width: {percentage:.1f}%" 
                     aria-valuenow="{score:.1f}" aria-valuemin="0" aria-valuemax="{max_score:.0f}"></div>
            </div>
        </div>'''
    
    def _generate_table_html(self, target_df: pd.DataFrame, pane_id: str, tab_id: str, highlight_type: str, is_active: bool = False) -> str:
        """
        テーブルHTMLを生成（共通メソッド）
        
        Args:
            target_df: 表示するDataFrame
            pane_id: タブペインのID
            tab_id: タブのID
            highlight_type: 強調タイプ ("all", "growth", "value")
            is_active: アクティブタブかどうか
            
        Returns:
            HTML文字列
        """
        active_class = "show active" if is_active else ""
        fade_class = "" if is_active else "fade"
        
        html = f"""                    <!-- {highlight_type}タブ -->
                    <div class="tab-pane {fade_class} {active_class}" id="{pane_id}" role="tabpanel" aria-labelledby="{tab_id}">
"""
        
        # 説明アラート
        if highlight_type == "growth":
            html += """                        <div class="alert alert-info mt-3">
                            <strong>🚀 グロース特化ランキング</strong><br>
                            成長性（売上成長率）と収益性（ROE）を重視したランキングです。割安度は度外視しています。
                        </div>
"""
        elif highlight_type == "value":
            html += """                        <div class="alert alert-warning mt-3">
                            <strong>💎 割安お宝株ランキング</strong><br>
                            割安度（PBR/PER）と安全性（自己資本比率）を重視したランキングです。成長性は度外視しています。
                        </div>
"""
        
        # テーブルのヘッダーを強調タイプに応じて変更
        if highlight_type == "growth":
            # グロースタブ：成長性列を追加して強調
            table_header = """                        <div class="table-responsive">
                            <table class="table table-striped table-hover">
                                <thead class="table-dark">
                                    <tr>
                                        <th>順位</th>
                                        <th>銘柄コード</th>
                                        <th>銘柄名</th>
                                        <th>業種</th>
                                        <th>総合スコア</th>
                                        <th class="table-primary">成長性スコア</th>
                                        <th>スコア内訳</th>
                                        <th>生データ</th>
                                        <th>チャート</th>
                                    </tr>
                                </thead>
                                <tbody>
"""
        elif highlight_type == "value":
            # バリュータブ：割安度列を追加して強調
            table_header = """                        <div class="table-responsive">
                            <table class="table table-striped table-hover">
                                <thead class="table-dark">
                                    <tr>
                                        <th>順位</th>
                                        <th>銘柄コード</th>
                                        <th>銘柄名</th>
                                        <th>業種</th>
                                        <th>総合スコア</th>
                                        <th class="table-warning">割安度スコア</th>
                                        <th>スコア内訳</th>
                                        <th>生データ</th>
                                        <th>チャート</th>
                                    </tr>
                                </thead>
                                <tbody>
"""
        else:
            # 総合タブ：通常のヘッダー
            table_header = """                        <div class="table-responsive">
                            <table class="table table-striped table-hover">
                                <thead class="table-dark">
                                    <tr>
                                        <th>順位</th>
                                        <th>銘柄コード</th>
                                        <th>銘柄名</th>
                                        <th>業種</th>
                                        <th>総合スコア</th>
                                        <th>スコア内訳</th>
                                        <th>生データ</th>
                                        <th>チャート</th>
                                    </tr>
                                </thead>
                                <tbody>
"""
        html += table_header
        
        # テーブル行を生成
        for idx, row in target_df.iterrows():
            html += self._generate_table_row(row, row.get('display_rank', idx + 1), highlight_type)
        
        html += """                                </tbody>
                            </table>
                        </div>
                    </div>
"""
        return html
    
    def _get_company_name_with_icons(self, row: pd.Series) -> str:
        """
        銘柄名にアイコンを追加
        
        Args:
            row: DataFrameの行
            
        Returns:
            アイコン付き銘柄名のHTML文字列
        """
        company_name = self._get_company_name(row.get('ticker', ''))
        icons = []
        
        # 成長性が高い（30点以上）場合は🔥
        score_growth = row.get('score_growth', 0)
        if score_growth is not None and not pd.isna(score_growth) and score_growth >= 30:
            icons.append('🔥')
        
        # 割安度が非常に高い（PBR < 0.8 または PER < 8）場合は💎
        pbr = row.get('pbr')
        per = row.get('per')
        if (pbr is not None and not pd.isna(pbr) and pbr < 0.8) or \
           (per is not None and not pd.isna(per) and per < 8):
            icons.append('💎')
        
        if icons:
            return f"{company_name} {' '.join(icons)}"
        return company_name
    
    def _generate_table_row_html(self, row: pd.Series, rank: int) -> str:
        """
        テーブル行を生成（新しい形式用）
        
        Args:
            row: DataFrameの行
            rank: 順位
            
        Returns:
            HTMLテーブル行の文字列
        """
        ticker = row.get('ticker', 'N/A')
        ticker_clean = re.sub(r'\.0$', '', str(ticker).replace('.T', '').replace('T', '').strip()).zfill(4)
        company_name = self._get_company_name(ticker)
        company_name_with_icons = self._get_company_name_with_icons(row)
        sector = self._get_sector(ticker)
        sector_display = sector if sector else "-"
        
        # スコア
        total_score = row.get('total_score', 0) or 0
        score_growth = row.get('score_growth', 0) or 0
        score_profit = row.get('score_profit', 0) or 0
        score_value = row.get('score_value', 0) or 0
        score_safety = row.get('score_safety', 0) or 0
        
        # スコアに応じた背景色クラス
        score_class = "score-high" if total_score >= 80 else "score-medium" if total_score >= 60 else "score-low"
        
        # 特徴/フラグ
        features = []
        if row.get('debt_free_flag') == True or row.get('is_debt_free') == True:
            features.append("💎無借金")
        if row.get('net_cash_status') == '実質無借金':
            features.append("💰キャッシュリッチ")
        revenue_growth_rate = row.get('revenue_growth_rate')
        if revenue_growth_rate is not None and not pd.isna(revenue_growth_rate) and revenue_growth_rate >= 10:
            features.append("🚀高成長")
        features_str = " ".join(features) if features else "-"
        
        return f"""
            <tr class="{score_class}">
                <td>{rank}</td>
                <td><strong>{ticker_clean}</strong><br><small>{company_name_with_icons}</small><br><small class="text-muted">{sector_display}</small></td>
                <td><span class="badge bg-success">{total_score:.1f}</span></td>
                <td>{score_growth:.1f}</td>
                <td>{score_profit:.1f}</td>
                <td>{score_value:.1f}</td>
                <td>{score_safety:.1f}</td>
                <td>{features_str}</td>
            </tr>
"""
    
    def _generate_table_row(self, row: pd.Series, rank: int, highlight_type: str = "all") -> str:
        """
        テーブル行を生成（共通メソッド）
        
        Args:
            row: DataFrameの行
            rank: 順位
            highlight_type: 強調タイプ ("all", "growth", "value")
            
        Returns:
            HTMLテーブル行の文字列
        """
        ticker = row.get('ticker', 'N/A')
        ticker_clean = re.sub(r'\.0$', '', str(ticker).replace('.T', '').replace('T', '').strip()).zfill(4)
        company_name_with_icons = self._get_company_name_with_icons(row)
        sector = self._get_sector(ticker)
        score = row.get('total_score', 0)
        
        # セクター情報を追加
        sector_display = sector if sector else "-"
        
        # スコアに応じた背景色クラス
        score_class = "score-high" if score >= 80 else "score-medium" if score >= 60 else "score-low"
        
        # スコアバッジ
        score_badge = f'<span class="badge bg-success">{score:.1f}</span>' if score >= 80 else f'<span class="badge bg-warning">{score:.1f}</span>' if score >= 60 else f'<span class="badge bg-secondary">{score:.1f}</span>'
        
        # スコア内訳（progress-bar）
        score_growth = row.get('score_growth', 0) or 0
        score_profit = row.get('score_profit', 0) or 0
        score_value = row.get('score_value', 0) or 0
        score_safety = row.get('score_safety', 0) or 0
        
        # 強調列の表示（グロースタブでは成長性スコア、バリュータブでは割安度スコア）
        highlight_score_cell = ""
        if highlight_type == "growth":
            # グロースタブ：成長性スコアを強調表示
            highlight_score_cell = f'<td class="table-primary"><strong>{score_growth:.1f}/40.0</strong></td>'
        elif highlight_type == "value":
            # バリュータブ：割安度スコアを強調表示
            highlight_score_cell = f'<td class="table-warning"><strong>{score_value:.1f}/20.0</strong></td>'
        
        score_breakdown = ""
        # 強調タイプに応じて背景色を変更
        if highlight_type == "growth":
            # グロース重視：成長性・ROEを強調
            score_breakdown += self._get_score_progress_bar(score_growth, 40.0, "成長性", highlight=True)
            score_breakdown += self._get_score_progress_bar(score_profit, 30.0, "ROE", highlight=True)
            score_breakdown += self._get_score_progress_bar(score_value, 20.0, "割安度")
            score_breakdown += self._get_score_progress_bar(score_safety, 10.0, "安全性")
        elif highlight_type == "value":
            # バリュー重視：割安度・安全性を強調
            score_breakdown += self._get_score_progress_bar(score_growth, 40.0, "成長性")
            score_breakdown += self._get_score_progress_bar(score_profit, 30.0, "ROE")
            score_breakdown += self._get_score_progress_bar(score_value, 20.0, "割安度", highlight=True)
            score_breakdown += self._get_score_progress_bar(score_safety, 10.0, "安全性", highlight=True)
        else:
            # 総合：すべて通常表示
            score_breakdown += self._get_score_progress_bar(score_growth, 40.0, "成長性")
            score_breakdown += self._get_score_progress_bar(score_profit, 30.0, "ROE")
            score_breakdown += self._get_score_progress_bar(score_value, 20.0, "割安度")
            score_breakdown += self._get_score_progress_bar(score_safety, 10.0, "安全性")
        
        # 生データの表示
        revenue_growth_rate = row.get('revenue_growth_rate')
        roe = row.get('roe')
        pbr = row.get('pbr')
        per = row.get('per')
        equity_ratio = row.get('equity_ratio')
        
        raw_data_html = "<small>"
        if revenue_growth_rate is not None and not pd.isna(revenue_growth_rate):
            raw_data_html += f"成長率: {revenue_growth_rate:+.1f}%<br>"
        if roe is not None and not pd.isna(roe):
            raw_data_html += f"ROE: {roe:.1f}%<br>"
        if pbr is not None and not pd.isna(pbr):
            raw_data_html += f"PBR: {pbr:.2f}<br>"
        if per is not None and not pd.isna(per):
            raw_data_html += f"PER: {per:.1f}倍<br>"
        if equity_ratio is not None and not pd.isna(equity_ratio):
            raw_data_html += f"自己資本比率: {equity_ratio:.1f}%"
        raw_data_html += "</small>"
        
        # Yahoo Financeボタン
        chart_button = self._get_yahoo_finance_button(ticker)
        
        # 強調列がある場合は追加
        if highlight_score_cell:
            return f"""                            <tr class="{score_class}">
                                <td>{rank}</td>
                                <td><strong>{ticker_clean}</strong></td>
                                <td>{company_name_with_icons}</td>
                                <td>{sector_display}</td>
                                <td>{score_badge}</td>
                                {highlight_score_cell}
                                <td>{score_breakdown}</td>
                                <td>{raw_data_html}</td>
                                <td>{chart_button}</td>
                            </tr>
"""
        else:
            return f"""                            <tr class="{score_class}">
                                <td>{rank}</td>
                                <td><strong>{ticker_clean}</strong></td>
                                <td>{company_name_with_icons}</td>
                                <td>{sector_display}</td>
                                <td>{score_badge}</td>
                                <td>{score_breakdown}</td>
                                <td>{raw_data_html}</td>
                                <td>{chart_button}</td>
                            </tr>
"""
    
    def _get_score_progress_bar(self, score: float, max_score: float, label: str, highlight: bool = False) -> str:
        """
        スコアをBootstrapのprogress-barで視覚化
        
        Args:
            score: 現在のスコア
            max_score: 満点
            label: ラベル名
            highlight: 強調表示するかどうか
            
        Returns:
            progress-barのHTML文字列
        """
        if max_score == 0:
            return f'<small class="text-muted">{label}: N/A</small>'
        
        percentage = min(100, max(0, (score / max_score) * 100))
        
        # 色を決定
        if highlight:
            # 強調表示：薄い青または黄色の背景
            bg_class = "bg-info" if percentage >= 60 else "bg-warning"
            container_style = "background-color: #e7f3ff; padding: 2px; border-radius: 4px;"
        else:
            if percentage >= 80:
                bg_class = "bg-success"
            elif percentage >= 60:
                bg_class = "bg-info"
            elif percentage >= 40:
                bg_class = "bg-warning"
            else:
                bg_class = "bg-danger"
            container_style = ""
        
        return f'''<div class="mb-1" style="{container_style}">
            <small><strong>{label}:</strong> {score:.1f}/{max_score:.0f}点</small>
            <div class="progress" style="height: 8px;">
                <div class="progress-bar {bg_class}" role="progressbar" style="width: {percentage:.1f}%" 
                     aria-valuenow="{score:.1f}" aria-valuemin="0" aria-valuemax="{max_score:.0f}"></div>
            </div>
        </div>'''
    
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
        
        if score >= 80:
            return "S"
        elif score >= 60:
            return "A"
        elif score >= 40:
            return "B"
        else:
            return "C"
    
    def _generate_table_row_html(self, row: pd.Series, rank: int) -> str:
        """
        テーブル行を生成（新しい形式用）
        
        Args:
            row: DataFrameの行
            rank: 順位
            
        Returns:
            HTMLテーブル行の文字列
        """
        ticker = row.get('ticker', 'N/A')
        ticker_clean = re.sub(r'\.0$', '', str(ticker).replace('.T', '').replace('T', '').strip()).zfill(4)
        company_name = self._get_company_name(ticker)
        company_name_with_icons = self._get_company_name_with_icons(row)
        sector = self._get_sector(ticker)
        sector_display = sector if sector else "-"
        
        # スコア
        total_score = row.get('total_score', 0) or 0
        score_growth = row.get('score_growth', 0) or 0
        score_profit = row.get('score_profit', 0) or 0
        score_value = row.get('score_value', 0) or 0
        score_safety = row.get('score_safety', 0) or 0
        
        # 実値データ
        revenue_growth_rate = row.get('revenue_growth_rate')
        roe = row.get('roe')
        pbr = row.get('pbr')
        per = row.get('per')
        equity_ratio = row.get('equity_ratio')
        operating_income = row.get('operating_income')
        revenue = row.get('revenue')
        
        # スコアに応じた背景色クラス
        score_class = "score-high" if total_score >= 80 else "score-medium" if total_score >= 60 else "score-low"
        
        # 特徴/フラグ
        features = []
        if row.get('debt_free_flag') == True or row.get('is_debt_free') == True:
            features.append("💎無借金")
        if row.get('net_cash_status') == '実質無借金':
            features.append("💰キャッシュリッチ")
        if revenue_growth_rate is not None and not pd.isna(revenue_growth_rate) and revenue_growth_rate >= 10:
            features.append("🚀高成長")
        features_str = " ".join(features) if features else "-"
        
        # スコアセルの生成（実値付き）
        # 成長性スコア
        growth_cell = f"{score_growth:.1f}"
        if revenue_growth_rate is not None and not pd.isna(revenue_growth_rate):
            growth_cell += f'<br><small class="text-muted">(成長率: {revenue_growth_rate:+.1f}%)</small>'
        
        # 収益性スコア
        profit_cell = f"{score_profit:.1f}"
        if roe is not None and not pd.isna(roe):
            profit_cell += f'<br><small class="text-muted">(ROE: {roe:.1f}%)</small>'
        
        # 割安度スコア
        value_cell = f"{score_value:.1f}"
        if pbr is not None and not pd.isna(pbr):
            value_cell += f'<br><small class="text-muted">(PBR: {pbr:.2f}倍</small>'
            if per is not None and not pd.isna(per):
                value_cell += f' / PER: {per:.1f}倍)'
            else:
                value_cell += ')'
        elif per is not None and not pd.isna(per):
            value_cell += f'<br><small class="text-muted">(PER: {per:.1f}倍)</small>'
        
        # 安全性スコア
        safety_cell = f"{score_safety:.1f}"
        if equity_ratio is not None and not pd.isna(equity_ratio):
            safety_cell += f'<br><small class="text-muted">(自己資本: {equity_ratio:.1f}%)</small>'
        
        # ツールチップ用の詳細情報を構築
        tooltip_parts = []
        if revenue is not None and not pd.isna(revenue):
            revenue_billion = revenue / 100000000  # 億円に変換
            tooltip_parts.append(f"売上高: {revenue_billion:.1f}億円")
        if operating_income is not None and not pd.isna(operating_income):
            op_income_billion = operating_income / 100000000  # 億円に変換
            tooltip_parts.append(f"営業利益: {op_income_billion:.1f}億円")
        if revenue is not None and not pd.isna(revenue) and operating_income is not None and not pd.isna(operating_income) and revenue != 0:
            op_margin = (operating_income / revenue) * 100
            tooltip_parts.append(f"営業利益率: {op_margin:.1f}%")
        if per is not None and not pd.isna(per):
            tooltip_parts.append(f"PER: {per:.1f}倍")
        if pbr is not None and not pd.isna(pbr):
            tooltip_parts.append(f"PBR: {pbr:.2f}倍")
        
        tooltip_text = " | ".join(tooltip_parts) if tooltip_parts else ""
        
        return f"""
            <tr class="{score_class}">
                <td>{rank}</td>
                <td><strong>{ticker_clean}</strong><br><small>{company_name_with_icons}</small><br><small class="text-muted">{sector_display}</small></td>
                <td><span class="badge bg-success">{total_score:.1f}</span></td>
                <td title="{tooltip_text}">{growth_cell}</td>
                <td title="{tooltip_text}">{profit_cell}</td>
                <td title="{tooltip_text}">{value_cell}</td>
                <td title="{tooltip_text}">{safety_cell}</td>
                <td>{features_str}</td>
            </tr>
"""
    
    def generate_html(self, df: pd.DataFrame) -> str:
        """HTMLレポートを生成（タブ切り替え対応版）"""
        if df.empty:
            return """<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>日本株 厳選成長銘柄スコアボード</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-4">
        <h1>推奨銘柄レポート</h1>
        <p>データがありません。</p>
    </div>
</body>
</html>"""
        
        # missing_criticalで分離
        if 'missing_critical' in df.columns:
            df['missing_critical'] = df['missing_critical'].astype(str).str.lower().isin(['true', '1', 'yes'])
            main_df = df[~df['missing_critical']].copy()
        else:
            main_df = df.copy()
        
        now = datetime.now()
        from datetime import timezone, timedelta
        jst = timezone(timedelta(hours=9))
        update_time_jst = now.astimezone(jst).strftime("%Y年%m月%d日 %H:%M JST")
        
        # 1. データの準備（3つの視点でソート）
        df_total = main_df.sort_values('total_score', ascending=False).head(100).copy()
        
        if 'score_growth' not in main_df.columns:
            main_df['score_growth'] = 0
        if 'score_profit' not in main_df.columns:
            main_df['score_profit'] = 0
        main_df['growth_score'] = main_df['score_growth'].fillna(0) + main_df['score_profit'].fillna(0)
        df_growth = main_df.sort_values('growth_score', ascending=False).head(100).copy()
        
        if 'score_value' not in main_df.columns:
            main_df['score_value'] = 0
        if 'score_safety' not in main_df.columns:
            main_df['score_safety'] = 0
        main_df['value_score'] = main_df['score_value'].fillna(0) + main_df['score_safety'].fillna(0)
        df_value = main_df.sort_values('value_score', ascending=False).head(100).copy()

        def create_full_table(target_df, table_id, highlight_type=None):
            rows = ""
            for i, (_, row) in enumerate(target_df.iterrows(), 1):
                rows += self._generate_table_row_html(row, i)
            
            # 強調する列をCSSで制御
            extra_style = ""
            if highlight_type == "growth":
                extra_style = f"<style>#{table_id} td:nth-child(4), #{table_id} td:nth-child(5) {{ background-color: rgba(13, 110, 253, 0.05); }}</style>"
            elif highlight_type == "value":
                extra_style = f"<style>#{table_id} td:nth-child(6), #{table_id} td:nth-child(7) {{ background-color: rgba(25, 135, 84, 0.05); }}</style>"

            return extra_style + f"""
            <div class="table-responsive">
                <table class="table table-hover border" id="{table_id}">
                    <thead class="table-dark">
                        <tr>
                            <th>順位</th>
                            <th>コード/名称</th>
                            <th>総合</th>
                            <th>成長性(40)</th>
                            <th>収益性(30)</th>
                            <th>割安度(20)</th>
                            <th>安全性(10)</th>
                            <th>特徴/フラグ</th>
                        </tr>
                    </thead>
                    <tbody>{rows}</tbody>
                </table>
            </div>
            """

        html = f"""
<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🇯🇵 日本株 戦略別スコアボード</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        .score-high {{ background-color: #d4edda !important; }}
        .score-medium {{ background-color: #fff3cd !important; }}
        .score-low {{ background-color: #f8d7da !important; }}
        .nav-tabs .nav-link {{ font-weight: bold; color: #666; }}
        .nav-tabs .nav-link.active {{ color: #0d6efd; border-bottom: 3px solid #0d6efd; }}
    </style>
</head>
<body class="bg-light">
    <div class="container-fluid mt-4">
        <h1 class="text-center mb-4">📊 日本株 戦略別ランキング</h1>
        
        <div class="text-center mb-4">
            <span class="badge bg-primary me-2">更新日時: {update_time_jst}</span>
            <span class="badge bg-success">対象: {len(main_df)}銘柄</span>
        </div>

        <ul class="nav nav-tabs mb-3" id="rankingTabs" role="tablist">
            <li class="nav-item">
                <button class="nav-link active" data-bs-toggle="tab" data-bs-target="#total" type="button">📊 総合</button>
            </li>
            <li class="nav-item">
                <button class="nav-link" data-bs-toggle="tab" data-bs-target="#growth" type="button">🚀 グロース特化</button>
            </li>
            <li class="nav-item">
                <button class="nav-link" data-bs-toggle="tab" data-bs-target="#value" type="button">💎 割安お宝株</button>
            </li>
        </ul>

        <div class="tab-content bg-white p-3 border border-top-0 rounded-bottom shadow-sm">
            <div class="tab-pane fade show active" id="total">
                <h4 class="mb-3">総合ランキング <small class="text-muted">(全指標のバランス重視)</small></h4>
                {create_full_table(df_total, "table-total")}
            </div>
            <div class="tab-pane fade" id="growth">
                <h4 class="mb-3 text-primary">🚀 グロースランキング <small class="text-muted">(成長性×収益性重視)</small></h4>
                {create_full_table(df_growth, "table-growth", "growth")}
            </div>
            <div class="tab-pane fade" id="value">
                <h4 class="mb-3 text-success">💎 割安お宝ランキング <small class="text-muted">(割安度×安全性重視)</small></h4>
                {create_full_table(df_value, "table-value", "value")}
            </div>
        </div>

        <div class="row mt-5 p-4 bg-white border rounded shadow-sm">
            <div class="col-md-6">
                <h4>📝 凡例</h4>
                <ul>
                    <li><strong>総合</strong>: 全指標（100点満点）の合計</li>
                    <li><strong>🚀 グロース</strong>: 成長性(40) + 収益性(30) の合計順</li>
                    <li><strong>💎 バリュー</strong>: 割安度(20) + 安全性(10) の合計順</li>
                </ul>
            </div>
        </div>
    </div>
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
        """
        return html
        
        # Sランク銘柄（Score 80+）を抽出（参考データを除く）- 新しいスコアリング方式
        s_rank_df = main_df[main_df.get('total_score', 0) >= 80].copy()
        
        if not s_rank_df.empty:
            html += """                <div class="table-responsive">
                    <table class="table table-striped table-hover">
                        <thead class="table-dark">
                            <tr>
                                <th>順位</th>
                                <th>銘柄コード</th>
                                <th>銘柄名</th>
                                <th>業種</th>
                                <th>総合スコア</th>
                                <th>スコア内訳</th>
                                <th>生データ</th>
                                <th>チャート</th>
                            </tr>
                        </thead>
                        <tbody>
"""
            for idx, row in s_rank_df.iterrows():
                rank = row.get('rank', idx + 1)
                ticker = row.get('ticker', 'N/A')
                ticker_clean = re.sub(r'\.0$', '', str(ticker).replace('.T', '').replace('T', '').strip()).zfill(4)
                company_name_with_icons = self._get_company_name_with_icons(row)
                sector = self._get_sector(ticker)
                score = row.get('total_score', 0)
                
                # スコアに応じた背景色クラス
                score_class = "score-high" if score >= 80 else "score-medium" if score >= 60 else "score-low"
                
                # スコアバッジ
                score_badge = f'<span class="badge bg-success">{score:.1f}</span>' if score >= 80 else f'<span class="badge bg-warning">{score:.1f}</span>' if score >= 60 else f'<span class="badge bg-secondary">{score:.1f}</span>'
                
                # スコア内訳（progress-bar）
                score_growth = row.get('score_growth', 0) or 0
                score_profit = row.get('score_profit', 0) or 0
                score_value = row.get('score_value', 0) or 0
                score_safety = row.get('score_safety', 0) or 0
                
                score_breakdown = ""
                score_breakdown += self._get_score_progress_bar(score_growth, 40.0, "成長性")
                score_breakdown += self._get_score_progress_bar(score_profit, 30.0, "ROE")
                score_breakdown += self._get_score_progress_bar(score_value, 20.0, "割安度")
                score_breakdown += self._get_score_progress_bar(score_safety, 10.0, "安全性")
                
                # 生データの表示
                revenue_growth_rate = row.get('revenue_growth_rate')
                roe = row.get('roe')
                pbr = row.get('pbr')
                per = row.get('per')
                equity_ratio = row.get('equity_ratio')
                
                raw_data_html = "<small>"
                if revenue_growth_rate is not None and not pd.isna(revenue_growth_rate):
                    raw_data_html += f"成長率: {revenue_growth_rate:+.1f}%<br>"
                if roe is not None and not pd.isna(roe):
                    raw_data_html += f"ROE: {roe:.1f}%<br>"
                if pbr is not None and not pd.isna(pbr):
                    raw_data_html += f"PBR: {pbr:.2f}<br>"
                if per is not None and not pd.isna(per):
                    raw_data_html += f"PER: {per:.1f}倍<br>"
                if equity_ratio is not None and not pd.isna(equity_ratio):
                    raw_data_html += f"自己資本比率: {equity_ratio:.1f}%"
                raw_data_html += "</small>"
                
                # 業種表示
                sector_display = sector if sector else "-"
                
                # Yahoo Financeボタン
                chart_button = self._get_yahoo_finance_button(ticker)
                
                html += f"""                            <tr class="{score_class}">
                                <td>{rank}</td>
                                <td><strong>{ticker_clean}</strong></td>
                                <td>{company_name_with_icons}</td>
                                <td>{sector_display}</td>
                                <td>{score_badge}</td>
                                <td>{score_breakdown}</td>
                                <td>{raw_data_html}</td>
                                <td>{chart_button}</td>
                            </tr>
"""
            html += """                        </tbody>
                    </table>
                </div>
"""
        else:
            html += """                <div class="alert alert-info">Sランク銘柄はありません。</div>
"""
        
        # ランキングの分類ロジック
        # 1. 総合ランキング（total_score順）
        df_total = main_df.copy()
        df_total = df_total.sort_values('total_score', ascending=False, na_position='last')
        df_total['display_rank'] = range(1, len(df_total) + 1)
        
        # 2. グロース特化（score_growth + score_profit の合計が高い順）
        df_growth = main_df.copy()
        # DataFrameの列に対してはfillna(0)を使用
        if 'score_growth' not in df_growth.columns:
            df_growth['score_growth'] = 0
        if 'score_profit' not in df_growth.columns:
            df_growth['score_profit'] = 0
        df_growth['growth_score'] = df_growth['score_growth'].fillna(0) + df_growth['score_profit'].fillna(0)
        df_growth = df_growth.sort_values('growth_score', ascending=False, na_position='last')
        df_growth['display_rank'] = range(1, len(df_growth) + 1)
        
        # 3. 割安お宝株（score_value + score_safety の合計が高い順）
        df_value = main_df.copy()
        # DataFrameの列に対してはfillna(0)を使用
        if 'score_value' not in df_value.columns:
            df_value['score_value'] = 0
        if 'score_safety' not in df_value.columns:
            df_value['score_safety'] = 0
        df_value['value_score'] = df_value['score_value'].fillna(0) + df_value['score_safety'].fillna(0)
        df_value = df_value.sort_values('value_score', ascending=False, na_position='last')
        df_value['display_rank'] = range(1, len(df_value) + 1)
        
        # Full Ranking Table with Tabs
        html += """                <hr>
                <h2 class="mt-4">📈 Full Ranking (全銘柄比較)</h2>
                
                <!-- Bootstrap Tabs -->
                <ul class="nav nav-tabs mb-3" id="rankingTabs" role="tablist">
                    <li class="nav-item" role="presentation">
                        <button class="nav-link active" id="total-tab" data-bs-toggle="tab" data-bs-target="#total-pane" type="button" role="tab" aria-controls="total-pane" aria-selected="true">
                            📊 総合
                        </button>
                    </li>
                    <li class="nav-item" role="presentation">
                        <button class="nav-link" id="growth-tab" data-bs-toggle="tab" data-bs-target="#growth-pane" type="button" role="tab" aria-controls="growth-pane" aria-selected="false">
                            🚀 グロース特化
                        </button>
                    </li>
                    <li class="nav-item" role="presentation">
                        <button class="nav-link" id="value-tab" data-bs-toggle="tab" data-bs-target="#value-pane" type="button" role="tab" aria-controls="value-pane" aria-selected="false">
                            💎 割安お宝株
                        </button>
                    </li>
                </ul>
                
                <div class="tab-content" id="rankingTabsContent">
"""
        
        # 総合ランキングタブ
        html += self._generate_table_html(df_total, "total-pane", "total-tab", "all", True)
        
        # グロース特化タブ
        html += self._generate_table_html(df_growth, "growth-pane", "growth-tab", "growth", False)
        
        # 割安お宝株タブ
        html += self._generate_table_html(df_value, "value-pane", "value-tab", "value", False)
        
        html += """                </div>
"""
        
        # 参考データセクション（missing_criticalがTrueの銘柄）
        if not reference_df.empty:
            html += """                <hr>
                <h2 class="mt-4">⚠️ 参考データ（重要データ欠損あり）</h2>
                <div class="alert alert-warning">以下の銘柄は重要な財務データが欠損しているため、参考情報として表示しています。</div>
                <div class="table-responsive">
                    <table class="table table-striped table-hover">
                        <thead class="table-dark">
                            <tr>
                                <th>順位</th>
                                <th>銘柄コード</th>
                                <th>銘柄名</th>
                                <th>業種</th>
                                <th>ROIC</th>
                                <th>売上成長率</th>
                                <th>総合スコア</th>
                                <th>チャート</th>
                                <th>欠損項目</th>
                            </tr>
                        </thead>
                        <tbody>
"""
            
            # 参考データのテーブル行を生成
            for idx, row in reference_df.iterrows():
                rank = row.get('rank', idx + 1)
                ticker = row.get('ticker', 'N/A')
                ticker_clean = re.sub(r'\.0$', '', str(ticker).replace('.T', '').replace('T', '').strip()).zfill(4)
                company_name = self._get_company_name(ticker)
                sector = self._get_sector(ticker)
                score = row.get('total_score', 0)
                roic = self._format_roic(row.get('roic'))
                growth_rate = self._format_growth_rate(row.get('revenue_growth_rate'))
                
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
                
                # Yahoo Financeボタン
                chart_button = self._get_yahoo_finance_button(ticker)
                
                # セクター情報
                sector_display = sector if sector else "-"
                
                html += f"""                            <tr>
                                <td>{rank}</td>
                                <td><strong>{ticker_clean}</strong></td>
                                <td>{company_name}</td>
                                <td>{sector_display}</td>
                                <td>{roic_str}</td>
                                <td>{growth_str}</td>
                                <td><span class="badge bg-secondary">{score:.0f}</span></td>
                                <td>{chart_button}</td>
                                <td><small>{missing_str}</small></td>
                            </tr>
"""
            
            html += """                        </tbody>
                    </table>
                </div>
"""
        
        html += """                <hr>
                <h2 class="mt-4">📈 Full Ranking (全銘柄比較)</h2>
                <div class="table-responsive">
                    <table class="table table-striped table-hover">
                        <thead class="table-dark">
                            <tr>
                                <th>順位</th>
                                <th>銘柄コード</th>
                                <th>銘柄名</th>
                                <th>業種</th>
                                <th>ROIC</th>
                                <th>売上成長率</th>
                                <th>総合スコア</th>
                                <th>チャート</th>
                            </tr>
                        </thead>
                        <tbody>
"""
        
        html += f"""                <hr>
                <div class="row mt-4">
                    <div class="col-md-6">
                        <h3>📝 凡例</h3>
                        <ul>
                            <li>💎無借金: 有利子負債がゼロの銘柄</li>
                            <li>💰キャッシュリッチ: 現預金が有利子負債を上回る銘柄（実質無借金）</li>
                            <li>🚀高成長: 売上成長率が10%を超える銘柄</li>
                        </ul>
                    </div>
                    <div class="col-md-6">
                        <h3>📊 スコアリング</h3>
                        <ul>
                            <li><strong>Sランク</strong>: 80点以上（優秀）</li>
                            <li><strong>Aランク</strong>: 60-79点（良好）</li>
                            <li><strong>Bランク</strong>: 40-59点（普通）</li>
                            <li><strong>Cランク</strong>: 40点未満（要改善）</li>
                        </ul>
                        <h4>スコアリング方式</h4>
                        <ul>
                            <li><strong>成長性</strong>: 売上成長率（40点満点）</li>
                            <li><strong>ROE</strong>: 自己資本利益率（30点満点）</li>
                            <li><strong>割安度</strong>: PBR/PER（20点満点）</li>
                            <li><strong>安全性</strong>: 自己資本比率（10点満点）</li>
                            <li><strong>減点</strong>: 営業利益マイナスまたは売上成長率マイナスで-40点</li>
                        </ul>
                        <h4>ランキングタブの説明</h4>
                        <ul>
                            <li><strong>📊 総合ランキング</strong>: 全指標を総合的に評価したランキング（total_score順）</li>
                            <li><strong>🚀 グロース特化</strong>: 成長性（売上成長率）と収益性（ROE）を重視したランキング。割安度は度外視しています。</li>
                            <li><strong>💎 割安お宝株</strong>: 割安度（PBR/PER）と安全性（自己資本比率）を重視したランキング。成長性は度外視しています。</li>
                        </ul>
                    </div>
                </div>
                <hr>
                <div class="text-center mt-4 mb-4">
                    <p class="text-muted"><strong>最終更新:</strong> {update_time_jst}</p>
                    <p class="text-muted"><strong>次回更新予定:</strong> {next_update}</p>
                    <p class="text-muted small mt-3">⚠️ <strong>免責事項:</strong> 本データは投資の助言ではありません。投資判断は自己責任でお願いいたします。</p>
                </div>
            </div>
        </div>
    </div>
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    <script>
        // Bootstrapタブの初期化（必要に応じて）
        var triggerTabList = [].slice.call(document.querySelectorAll('#rankingTabs button'));
        triggerTabList.forEach(function (triggerEl) {{
            var tabTrigger = new bootstrap.Tab(triggerEl);
            triggerEl.addEventListener('click', function (event) {{
                event.preventDefault();
                tabTrigger.show();
            }});
        }});
    </script>
</body>
</html>"""
        
        return html
    
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
        
        # HTMLを生成
        html = self.generate_html(df)
        
        # ファイルに保存
        output_path = self.output_dir / "index.html"
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html)
            logger.info(f"レポート保存完了: {output_path}")
            return str(output_path)
        except PermissionError:
            logger.error(f"ファイルが他のプロセスで開かれています: {output_path}")
            logger.error("ブラウザやエディタでファイルを閉じてから再実行してください。")
            return ""
        except Exception as e:
            logger.error(f"ファイル保存エラー: {str(e)}")
            return ""


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
