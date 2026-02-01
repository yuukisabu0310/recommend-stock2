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
    
    def _format_millions(self, value: Optional[float]) -> str:
        """
        値を百万円単位で3桁カンマ区切りに整形
        
        Args:
            value: 元の値
            
        Returns:
            百万円単位の文字列（例: "1,234.5百万円"）
        """
        if value is None or pd.isna(value):
            return "-"
        # 百万円単位に変換
        millions = value / 1000000
        # 3桁カンマ区切りで整形
        return f"{millions:,.1f}百万円"
    
    def _format_millions_with_color(self, value: Optional[float], is_positive_good: bool = True) -> str:
        """
        値を百万円単位で3桁カンマ区切りに整形し、色分け
        
        Args:
            value: 元の値
            is_positive_good: Trueの場合、プラスが良い（利益など）、Falseの場合、マイナスが良い（負債など）
            
        Returns:
            色分けされたHTML文字列
        """
        if value is None or pd.isna(value):
            return '<span class="text-muted">-</span>'
        
        millions = value / 1000000
        formatted = f"{millions:,.1f}百万円"
        
        if is_positive_good:
            if millions >= 0:
                return f'<span class="text-dark">{formatted}</span>'
            else:
                return f'<span class="text-danger">{formatted}</span>'
        else:
            if millions <= 0:
                return f'<span class="text-dark">{formatted}</span>'
            else:
                return f'<span class="text-danger">{formatted}</span>'
    
    def _generate_stock_modal_html(self, row: pd.Series, prefix: str) -> str:
        """
        銘柄詳細モーダルのHTMLを生成
        
        Args:
            row: DataFrameの行
            prefix: タブのプレフィックス（'value'または'growth'）
            
        Returns:
            モーダルのHTML文字列
        """
        ticker = row.get('ticker', 'N/A')
        ticker_clean = re.sub(r'\.0$', '', str(ticker).replace('.T', '').replace('T', '').strip()).zfill(4)
        company_name = self._get_company_name(ticker)
        modal_id = f"modal-{prefix}-{ticker_clean}"
        
        # 全指標を取得
        per = row.get('per')
        pbr = row.get('pbr')
        roe = row.get('roe')
        equity_ratio = row.get('equity_ratio')
        eps = row.get('eps')
        bps = row.get('bps')
        dividend_yield = row.get('dividend_yield')
        revenue_growth_rate = row.get('revenue_growth_rate')
        operating_margin = row.get('operating_margin')
        market_cap = row.get('market_cap')
        sector = row.get('sector')
        
        # BS項目
        total_assets = row.get('total_assets')
        total_liabilities = row.get('total_liabilities')
        equity = row.get('equity')
        
        # PL項目
        revenue = row.get('revenue')
        cost_of_revenue = row.get('cost_of_revenue')
        gross_profit = row.get('gross_profit')
        sga = row.get('sga')
        operating_income = row.get('operating_income')
        ordinary_income = row.get('ordinary_income')
        pretax_income = row.get('pretax_income')
        tax_provision = row.get('tax_provision')
        net_income = row.get('net_income')
        
        # CF項目
        beginning_cash_balance = row.get('beginning_cash_balance')
        operating_cash_flow = row.get('operating_cash_flow')
        investing_cash_flow = row.get('investing_cash_flow')
        financing_cash_flow = row.get('financing_cash_flow')
        end_cash_value = row.get('end_cash_value')
        
        # 指標リストの生成（2列グリッド形式）
        per_str = f"{per:.1f}倍" if per is not None and not pd.isna(per) else "N/A"
        pbr_str = f"{pbr:.2f}倍" if pbr is not None and not pd.isna(pbr) else "N/A"
        roe_str = f"{roe:.1f}%" if roe is not None and not pd.isna(roe) else "N/A"
        equity_ratio_str = f"{equity_ratio:.1f}%" if equity_ratio is not None and not pd.isna(equity_ratio) else "N/A"
        eps_str = f"{eps:.0f}円" if eps is not None and not pd.isna(eps) else "N/A"
        bps_str = f"{bps:.0f}円" if bps is not None and not pd.isna(bps) else "N/A"
        dividend_yield_str = f"{dividend_yield:.2f}%" if dividend_yield is not None and not pd.isna(dividend_yield) else "N/A"
        revenue_growth_str = f"{revenue_growth_rate:.1f}%" if revenue_growth_rate is not None and not pd.isna(revenue_growth_rate) else "N/A"
        operating_margin_str = f"{operating_margin:.1f}%" if operating_margin is not None and not pd.isna(operating_margin) else "N/A"
        market_cap_str = f"{market_cap:.1f}億円" if market_cap is not None and not pd.isna(market_cap) else "N/A"
        sector_str = str(sector) if sector is not None and not pd.isna(sector) else "N/A"
        
        # サマリーカード
        summary_card = f"""
            <div class="modal-summary-card">
                <div class="summary-item">
                    <span class="summary-label">銘柄コード</span>
                    <span class="summary-value">{ticker_clean}</span>
                </div>
                <div class="summary-item">
                    <span class="summary-label">銘柄名</span>
                    <span class="summary-value">{company_name}</span>
                </div>
                <div class="summary-item">
                    <span class="summary-label">業種</span>
                    <span class="summary-value">{sector_str}</span>
                </div>
                <div class="summary-item">
                    <span class="summary-label">時価総額</span>
                    <span class="summary-value">{market_cap_str}</span>
                </div>
            </div>
        """
        
        # 財務スコア（2列グリッド）
        financial_scores = f"""
            <div class="modal-financial-scores">
                <h6 class="modal-section-title">財務スコア</h6>
                <div class="scores-grid">
                    <div class="score-item">
                        <span class="score-label">PER</span>
                        <span class="score-value">{per_str}</span>
                    </div>
                    <div class="score-item">
                        <span class="score-label">PBR</span>
                        <span class="score-value">{pbr_str}</span>
                    </div>
                    <div class="score-item">
                        <span class="score-label">ROE</span>
                        <span class="score-value">{roe_str}</span>
                    </div>
                    <div class="score-item">
                        <span class="score-label">自己資本比率</span>
                        <span class="score-value">{equity_ratio_str}</span>
                    </div>
                    <div class="score-item">
                        <span class="score-label">EPS</span>
                        <span class="score-value">{eps_str}</span>
                    </div>
                    <div class="score-item">
                        <span class="score-label">BPS</span>
                        <span class="score-value">{bps_str}</span>
                    </div>
                    <div class="score-item">
                        <span class="score-label">配当利回り</span>
                        <span class="score-value">{dividend_yield_str}</span>
                    </div>
                    <div class="score-item">
                        <span class="score-label">売上成長率</span>
                        <span class="score-value">{revenue_growth_str}</span>
                    </div>
                    <div class="score-item">
                        <span class="score-label">営業利益率</span>
                        <span class="score-value">{operating_margin_str}</span>
                    </div>
                </div>
            </div>
        """
        
        # 業績推移テーブル（BS/PL/CF）
        financial_table = f"""
            <div class="modal-financial-table">
                <h6 class="modal-section-title">業績推移</h6>
                <div class="table-responsive">
                    <table class="table table-sm table-bordered">
                        <thead>
                            <tr>
                                <th>項目</th>
                                <th class="text-end">金額（百万円）</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr><td><strong>📊 貸借対照表 (BS)</strong></td><td></td></tr>
                            <tr><td>資産合計</td><td class="text-end">{self._format_millions_with_color(total_assets, True)}</td></tr>
                            <tr><td>負債合計</td><td class="text-end">{self._format_millions_with_color(total_liabilities, False)}</td></tr>
                            <tr><td>純資産合計</td><td class="text-end">{self._format_millions_with_color(equity, True)}</td></tr>
                            <tr><td><strong>💰 損益計算書 (PL)</strong></td><td></td></tr>
                            <tr><td>売上高</td><td class="text-end">{self._format_millions_with_color(revenue, True)}</td></tr>
                            <tr><td>売上原価</td><td class="text-end">{self._format_millions_with_color(cost_of_revenue, False)}</td></tr>
                            <tr><td>売上総利益</td><td class="text-end">{self._format_millions_with_color(gross_profit, True)}</td></tr>
                            <tr><td>販管費</td><td class="text-end">{self._format_millions_with_color(sga, False)}</td></tr>
                            <tr><td>営業利益</td><td class="text-end">{self._format_millions_with_color(operating_income, True)}</td></tr>
                            <tr><td>経常利益</td><td class="text-end">{self._format_millions_with_color(ordinary_income, True)}</td></tr>
                            <tr><td>税引前利益</td><td class="text-end">{self._format_millions_with_color(pretax_income, True)}</td></tr>
                            <tr><td>法人税等</td><td class="text-end">{self._format_millions_with_color(tax_provision, False)}</td></tr>
                            <tr><td>当期純利益</td><td class="text-end">{self._format_millions_with_color(net_income, True)}</td></tr>
                            <tr><td><strong>💵 キャッシュフロー (CF)</strong></td><td></td></tr>
                            <tr><td>期首残高</td><td class="text-end">{self._format_millions_with_color(beginning_cash_balance, True)}</td></tr>
                            <tr><td>営業CF</td><td class="text-end">{self._format_millions_with_color(operating_cash_flow, True)}</td></tr>
                            <tr><td>投資CF</td><td class="text-end">{self._format_millions_with_color(investing_cash_flow, True)}</td></tr>
                            <tr><td>財務CF</td><td class="text-end">{self._format_millions_with_color(financing_cash_flow, True)}</td></tr>
                            <tr><td>期末残高</td><td class="text-end">{self._format_millions_with_color(end_cash_value, True)}</td></tr>
                        </tbody>
                    </table>
                </div>
            </div>
        """
        
        return f"""
        <!-- Modal -->
        <div class="modal fade" id="{modal_id}" tabindex="-1" aria-labelledby="{modal_id}Label" aria-hidden="true">
            <div class="modal-dialog modal-lg modal-dialog-scrollable">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title" id="{modal_id}Label">
                            <span class="ticker-code-large">{ticker_clean}</span> {company_name}
                        </h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                    </div>
                    <div class="modal-body">
                        {summary_card}
                        {financial_scores}
                        {financial_table}
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">閉じる</button>
                    </div>
                </div>
            </div>
        </div>
        """
    
    def _generate_financial_details_html(self, row: pd.Series, prefix: str, colspan: int = 10) -> str:
        """
        財務詳細（BS/PL/CF）のHTMLを生成
        
        Args:
            row: DataFrameの行
            prefix: タブのプレフィックス（'value'または'growth'）
            colspan: テーブルの列数
            
        Returns:
            財務詳細のHTML文字列
        """
        # tickerを取得して整形
        ticker = row.get('ticker', 'N/A')
        ticker_clean = re.sub(r'\.0$', '', str(ticker).replace('.T', '').replace('T', '').strip()).zfill(4)
        
        # BS項目
        total_assets = row.get('total_assets')
        total_liabilities = row.get('total_liabilities')
        equity = row.get('equity')
        
        # PL項目
        revenue = row.get('revenue')
        cost_of_revenue = row.get('cost_of_revenue')
        gross_profit = row.get('gross_profit')
        sga = row.get('sga')
        operating_income = row.get('operating_income')
        ordinary_income = row.get('ordinary_income')
        pretax_income = row.get('pretax_income')
        tax_provision = row.get('tax_provision')
        net_income = row.get('net_income')
        
        # CF項目
        beginning_cash_balance = row.get('beginning_cash_balance')
        operating_cash_flow = row.get('operating_cash_flow')
        investing_cash_flow = row.get('investing_cash_flow')
        financing_cash_flow = row.get('financing_cash_flow')
        end_cash_value = row.get('end_cash_value')
        
        # BSセクション
        bs_html = f"""
            <div class="col-md-4">
                <h6 class="fw-bold text-primary mb-2">📊 貸借対照表 (BS)</h6>
                <table class="table table-sm table-bordered">
                    <tr>
                        <td class="fw-bold">資産合計</td>
                        <td class="text-end">{self._format_millions_with_color(total_assets, True)}</td>
                    </tr>
                    <tr>
                        <td class="fw-bold">負債合計</td>
                        <td class="text-end">{self._format_millions_with_color(total_liabilities, False)}</td>
                    </tr>
                    <tr>
                        <td class="fw-bold">純資産合計</td>
                        <td class="text-end">{self._format_millions_with_color(equity, True)}</td>
                    </tr>
                </table>
            </div>
        """
        
        # PLセクション
        pl_html = f"""
            <div class="col-md-4">
                <h6 class="fw-bold text-success mb-2">💰 損益計算書 (PL)</h6>
                <table class="table table-sm table-bordered">
                    <tr>
                        <td class="fw-bold">売上高</td>
                        <td class="text-end">{self._format_millions_with_color(revenue, True)}</td>
                    </tr>
                    <tr>
                        <td class="fw-bold">売上原価</td>
                        <td class="text-end">{self._format_millions_with_color(cost_of_revenue, False)}</td>
                    </tr>
                    <tr>
                        <td class="fw-bold">売上総利益</td>
                        <td class="text-end">{self._format_millions_with_color(gross_profit, True)}</td>
                    </tr>
                    <tr>
                        <td class="fw-bold">販管費</td>
                        <td class="text-end">{self._format_millions_with_color(sga, False)}</td>
                    </tr>
                    <tr>
                        <td class="fw-bold">営業利益</td>
                        <td class="text-end">{self._format_millions_with_color(operating_income, True)}</td>
                    </tr>
                    <tr>
                        <td class="fw-bold">経常利益</td>
                        <td class="text-end">{self._format_millions_with_color(ordinary_income, True)}</td>
                    </tr>
                    <tr>
                        <td class="fw-bold">税引前利益</td>
                        <td class="text-end">{self._format_millions_with_color(pretax_income, True)}</td>
                    </tr>
                    <tr>
                        <td class="fw-bold">法人税等</td>
                        <td class="text-end">{self._format_millions_with_color(tax_provision, False)}</td>
                    </tr>
                    <tr>
                        <td class="fw-bold">当期純利益</td>
                        <td class="text-end">{self._format_millions_with_color(net_income, True)}</td>
                    </tr>
                </table>
            </div>
        """
        
        # CFセクション
        cf_html = f"""
            <div class="col-md-4">
                <h6 class="fw-bold text-info mb-2">💵 キャッシュフロー (CF)</h6>
                <table class="table table-sm table-bordered">
                    <tr>
                        <td class="fw-bold">期首残高</td>
                        <td class="text-end">{self._format_millions_with_color(beginning_cash_balance, True)}</td>
                    </tr>
                    <tr>
                        <td class="fw-bold">営業CF</td>
                        <td class="text-end">{self._format_millions_with_color(operating_cash_flow, True)}</td>
                    </tr>
                    <tr>
                        <td class="fw-bold">投資CF</td>
                        <td class="text-end">{self._format_millions_with_color(investing_cash_flow, True)}</td>
                    </tr>
                    <tr>
                        <td class="fw-bold">財務CF</td>
                        <td class="text-end">{self._format_millions_with_color(financing_cash_flow, True)}</td>
                    </tr>
                    <tr>
                        <td class="fw-bold">期末残高</td>
                        <td class="text-end">{self._format_millions_with_color(end_cash_value, True)}</td>
                    </tr>
                </table>
            </div>
        """
        
        # テーブルの列数に応じてcolspanを設定（デフォルトは10列）
        colspan = 10
        
        detail_id = f"{prefix}-details-{ticker_clean}"
        return f"""
            <tr class="financial-details-row" id="{detail_id}" style="display: none;">
                <td colspan="{colspan}">
                    <div class="row p-3 bg-light border-top">
                        {bs_html}
                        {pl_html}
                        {cf_html}
                    </div>
                </td>
            </tr>
        """
    
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
        セクター（17業種区分を優先、なければ33業種区分）情報を読み込む
        
        Returns:
            ticker -> sector_name の辞書
        """
        sector_info = {}
        
        # jpx_tse_info.csvを読み込み
        jpx_info_path = self.raw_data_dir / "jpx_tse_info.csv"
        if jpx_info_path.exists():
            try:
                jpx_df = pd.read_csv(jpx_info_path, encoding='utf-8-sig')
                
                # 「コード」カラムの確認
                if 'コード' not in jpx_df.columns:
                    logger.error("「コード」列が見つかりません")
                    return {}
                
                # 17業種区分を優先、なければ33業種区分を使用
                sector_col = None
                if '17業種区分' in jpx_df.columns:
                    sector_col = '17業種区分'
                elif '33業種区分' in jpx_df.columns:
                    sector_col = '33業種区分'
                
                if not sector_col:
                    logger.error("「17業種区分」または「33業種区分」列が見つかりません")
                    return {}
                
                # 内国株式のみをフィルタリング
                if '市場・商品区分' in jpx_df.columns:
                    jpx_df = jpx_df[jpx_df['市場・商品区分'].astype(str).str.contains('内国株式', na=False)]
                
                for _, row in jpx_df.iterrows():
                    ticker = str(row['コード']).strip()
                    sector = str(row[sector_col]).strip()
                    # コード整形：.0$を正規表現で除去し、4桁の文字列（0埋め）に変換
                    ticker_clean = re.sub(r'\.0$', '', str(ticker)).strip()
                    # 4桁に整形
                    ticker_clean = ticker_clean.zfill(4)
                    if ticker_clean and sector and sector != '-' and ticker_clean.isdigit() and len(ticker_clean) == 4:
                        sector_info[ticker_clean] = sector
                logger.info(f"セクター情報を読み込みました（{sector_col}）: {len(sector_info)}件")
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
    
    def _generate_sector_select(self, select_id: str, sectors: List[str]) -> str:
        """
        業種セレクトボックスを生成
        
        Args:
            select_id: セレクトボックスのID
            sectors: 業種リスト
            
        Returns:
            HTMLセレクトボックスの文字列
        """
        options = ['<option value="all">すべての業種</option>']
        for sector in sectors:
            options.append(f'<option value="{sector}">{sector}</option>')
        
        return f'<select id="{select_id}" class="sector-filter-select" name="{select_id}">{"".join(options)}</select>'
    
    def _get_stock_marks(self, row: pd.Series) -> str:
        """
        銘柄のマーク（記号）を取得
        
        Args:
            row: DataFrameの行
            
        Returns:
            マークの文字列（複数の場合はスペース区切り）
        """
        marks = []
        
        # 🔥 : 売上成長率 20%以上
        revenue_growth_rate = row.get('revenue_growth_rate')
        if revenue_growth_rate is not None and not pd.isna(revenue_growth_rate) and revenue_growth_rate >= 20.0:
            marks.append('🔥')
        
        # 💎 : 完全無借金 (debt_free_flag または is_debt_free のいずれかがTrue)
        # debt_free_flag と is_debt_free の判定を統合
        debt_free_flag = row.get('debt_free_flag')
        is_debt_free = row.get('is_debt_free')
        is_completely_debt_free = (debt_free_flag == True) or (is_debt_free == True)
        
        # 💰 : 実質無借金 (net_cash_status または net_cash_flag)
        net_cash_status = row.get('net_cash_status')
        net_cash_flag = row.get('net_cash_flag')
        is_net_cash = (net_cash_status == '実質無借金') or (net_cash_flag == True)
        
        # 無借金（💎）と実質無借金（💰）が両方該当する場合は、より上位の概念である💎のみを表示
        if is_completely_debt_free:
            marks.append('💎')
        elif is_net_cash:
            marks.append('💰')
        
        # ⚠️ : 重要データ欠損 (missing_critical)
        if row.get('missing_critical') == True:
            marks.append('⚠️')
        
        return ' '.join(marks) if marks else ''
    
    def _get_cap_size_badge(self, cap_size: Optional[str]) -> str:
        """
        時価総額によるサイズ分類のバッジを取得
        
        Args:
            cap_size: サイズ分類（"大型", "中型", "小型"）
            
        Returns:
            バッジのHTML文字列
        """
        if cap_size is None or pd.isna(cap_size):
            return ""
        
        cap_size_str = str(cap_size).strip()
        
        if cap_size_str == '大型':
            return '<span class="badge bg-primary">大型</span>'
        elif cap_size_str == '中型':
            return '<span class="badge bg-info text-dark">中型</span>'
        elif cap_size_str == '小型':
            return '<span class="badge bg-secondary">小型</span>'
        else:
            return ""
    
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
        Yahoo FinanceへのリンクをYahoo Finance風ボタン形式で生成
        
        Args:
            ticker: 銘柄コード
            
        Returns:
            HTMLボタン形式の文字列
        """
        url = self._get_yahoo_finance_link(ticker)
        return f'<a href="{url}" target="_blank" class="btn-yahoo">📈 チャート</a>'
    
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
    
    def _generate_value_table_row_html(self, row: pd.Series, rank: int) -> str:
        """
        バリュー株用テーブル行を生成（3列構成：銘柄、主要指標、時価総額）
        
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
        
        # CSVから直接業種を取得（なければ_get_sectorで取得）
        sector = row.get('sector')
        if sector is None or pd.isna(sector) or str(sector).strip() == '':
            sector = self._get_sector(ticker)
        sector_display = str(sector).strip() if sector and not pd.isna(sector) else "-"
        
        # バリュー株で重要な指標
        dividend_yield = row.get('dividend_yield')
        pbr = row.get('pbr')
        market_cap = row.get('market_cap')
        
        # マークを取得
        marks = self._get_stock_marks(row)
        
        # 時価総額によるサイズ分類バッジを取得
        cap_size = row.get('cap_size')
        size_badge = self._get_cap_size_badge(cap_size)
        
        # Yahoo Financeリンク
        yahoo_url = self._get_yahoo_finance_link(ticker)
        
        # 銘柄セル：サイズバッジ + コード（リンク）+ 名称（リンクなし）+ 記号
        marks_display = f" {marks}" if marks else ""
        
        # 主要指標：利回り / PBR（下に指標名を表示）
        dividend_yield_display = f"{dividend_yield:.2f}%" if dividend_yield is not None and not pd.isna(dividend_yield) else "N/A"
        pbr_display = f"{pbr:.2f}x" if pbr is not None and not pd.isna(pbr) else "N/A"
        main_metrics = f"""
            <div class="metrics-value">{dividend_yield_display} / {pbr_display}</div>
            <div class="metrics-label">利回り / PBR</div>
        """
        
        # 時価総額の表示（億円単位）
        market_cap_display = f"{market_cap:.1f}" if market_cap is not None and not pd.isna(market_cap) else "-"
        
        # モーダルID
        modal_id = f"modal-value-{ticker_clean}"
        details_button_id = f"details-btn-value-{ticker_clean}"
        
        return f"""
            <tr class="stock-row" data-sector="{sector_display}" data-ticker="{ticker_clean}">
                <td class="stock-cell">
                    <div class="stock-info">
                        {size_badge if size_badge else ""}
                        <a href="{yahoo_url}" target="_blank" class="ticker-code-link">{ticker_clean}</a>
                        <span class="company-name">{company_name}{marks_display}</span>
                    </div>
                    <button class="btn-analyze" onclick="showModal('{modal_id}')" id="{details_button_id}" title="詳細を表示">ANALYZE</button>
                </td>
                <td class="metrics-cell">{main_metrics}</td>
                <td class="market-cap-cell">
                    <div class="market-cap-value">{market_cap_display}</div>
                    <div class="market-cap-label">億円</div>
                </td>
            </tr>
"""
    
    def _generate_growth_table_row_html(self, row: pd.Series, rank: int) -> str:
        """
        グロース株用テーブル行を生成（3列構成：銘柄、主要指標、時価総額）
        
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
        
        # CSVから直接業種を取得（なければ_get_sectorで取得）
        sector = row.get('sector')
        if sector is None or pd.isna(sector) or str(sector).strip() == '':
            sector = self._get_sector(ticker)
        sector_display = str(sector).strip() if sector and not pd.isna(sector) else "-"
        
        # グロース株で重要な指標
        revenue_growth_rate = row.get('revenue_growth_rate')
        operating_margin = row.get('operating_margin')
        market_cap = row.get('market_cap')
        
        # マークを取得
        marks = self._get_stock_marks(row)
        
        # 時価総額によるサイズ分類バッジを取得
        cap_size = row.get('cap_size')
        size_badge = self._get_cap_size_badge(cap_size)
        
        # Yahoo Financeリンク
        yahoo_url = self._get_yahoo_finance_link(ticker)
        
        # 銘柄セル：サイズバッジ + コード（リンク）+ 名称（リンクなし）+ 記号
        marks_display = f" {marks}" if marks else ""
        
        # 主要指標：成長率 / 営業利益率（下に指標名を表示）
        if revenue_growth_rate is not None and not pd.isna(revenue_growth_rate):
            revenue_growth_value = float(revenue_growth_rate)
            if revenue_growth_value >= 0:
                revenue_growth_display = f'<span class="growth-positive">{revenue_growth_rate:.1f}%</span>'
            else:
                revenue_growth_display = f'<span class="growth-negative">{revenue_growth_rate:.1f}%</span>'
        else:
            revenue_growth_display = "N/A"
        operating_margin_display = f"{operating_margin:.1f}%" if operating_margin is not None and not pd.isna(operating_margin) else "N/A"
        main_metrics = f"""
            <div class="metrics-value">{revenue_growth_display} / {operating_margin_display}</div>
            <div class="metrics-label">売上成長 / 営業利益率</div>
        """
        
        # 時価総額の表示（億円単位）
        market_cap_display = f"{market_cap:.1f}" if market_cap is not None and not pd.isna(market_cap) else "-"
        
        # モーダルID
        modal_id = f"modal-growth-{ticker_clean}"
        details_button_id = f"details-btn-growth-{ticker_clean}"
        
        return f"""
            <tr class="stock-row" data-sector="{sector_display}" data-ticker="{ticker_clean}">
                <td class="stock-cell">
                    <div class="stock-info">
                        {size_badge if size_badge else ""}
                        <a href="{yahoo_url}" target="_blank" class="ticker-code-link">{ticker_clean}</a>
                        <span class="company-name">{company_name}{marks_display}</span>
                    </div>
                    <button class="btn-analyze" onclick="showModal('{modal_id}')" id="{details_button_id}" title="詳細を表示">ANALYZE</button>
                </td>
                <td class="metrics-cell">{main_metrics}</td>
                <td class="market-cap-cell">
                    <div class="market-cap-value">{market_cap_display}</div>
                    <div class="market-cap-label">億円</div>
                </td>
            </tr>
"""
    
    def generate_html(self, value_df: pd.DataFrame, growth_df: pd.DataFrame) -> str:
        """HTMLレポートを生成（バリュー株・グロース株2タブ構成）"""
        if value_df.empty and growth_df.empty:
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
        
        now = datetime.now()
        from datetime import timezone, timedelta
        jst = timezone(timedelta(hours=9))
        update_time_jst = now.astimezone(jst).strftime("%Y年%m月%d日 %H:%M JST")
        
        # 業種リストを取得（五十音順でソート）
        value_sectors = []
        if not value_df.empty and 'sector' in value_df.columns:
            value_sectors = sorted([s for s in value_df['sector'].dropna().unique() if s and str(s).strip() != '-'])
        
        growth_sectors = []
        if not growth_df.empty and 'sector' in growth_df.columns:
            growth_sectors = sorted([s for s in growth_df['sector'].dropna().unique() if s and str(s).strip() != '-'])
        
        # バリュー株テーブルの行とモーダルを生成
        value_rows = ""
        value_modals = ""
        if not value_df.empty:
            for i, (_, row) in enumerate(value_df.iterrows(), 1):
                value_rows += self._generate_value_table_row_html(row, i)
                value_modals += self._generate_stock_modal_html(row, "value")
        
        # グロース株テーブルの行とモーダルを生成
        growth_rows = ""
        growth_modals = ""
        if not growth_df.empty:
            for i, (_, row) in enumerate(growth_df.iterrows(), 1):
                growth_rows += self._generate_growth_table_row_html(row, i)
                growth_modals += self._generate_stock_modal_html(row, "growth")
        
        # 業種セレクトボックスを生成
        value_sector_select = self._generate_sector_select('value-sector-filter', value_sectors)
        growth_sector_select = self._generate_sector_select('growth-sector-filter', growth_sectors)
        
        html = f"""
<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>J-Equity Insight Engine</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        /* Yahoo Finance US風カラーパレット */
        :root {{
            --yahoo-navy: #001c44;
            --yahoo-blue: #0081f1;
            --yahoo-green: #00b061;
            --yahoo-bg: #f6f9fc;
            --yahoo-border: #e0e4e9;
            --yahoo-text-gray: #666;
        }}
        
        body {{
            background-color: var(--yahoo-bg);
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
        }}
        
        /* メインコンテンツ */
        .main-content {{
            padding: 1.5rem 2rem;
            max-width: 100%;
        }}
        
        /* ヘッダー（ダッシュボード風） */
        .main-header {{
            background-color: var(--yahoo-navy);
            color: white;
            padding: 0;
            margin: 0;
            border-bottom: 2px solid var(--yahoo-border);
        }}
        .header-content {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 1rem 2rem;
            max-width: 100%;
        }}
        .header-title {{
            font-size: 1.5rem;
            font-weight: 700;
            margin: 0;
            color: white;
            letter-spacing: 0.05em;
        }}
        .header-strategy {{
            font-size: 0.9rem;
            font-weight: 600;
            color: rgba(255, 255, 255, 0.9);
            letter-spacing: 0.1em;
            text-transform: uppercase;
        }}
        .header-strategy.value {{
            color: #ffd700;
        }}
        .header-strategy.growth {{
            color: #00ff88;
        }}
        
        /* 凡例行（タブナビゲーションの下） */
        .legend-row {{
            padding: 0.75rem 0;
            margin-bottom: 1rem;
            font-size: 0.85rem;
            color: var(--yahoo-text-gray);
        }}
        .legend-row strong {{
            color: var(--yahoo-text-gray);
        }}
        .legend-row span {{
            margin-right: 1rem;
        }}
        
        /* フィルター行 */
        .filter-row {{
            display: flex;
            align-items: center;
            gap: 0.5rem;
            margin-bottom: 1rem;
            padding: 0.75rem 0;
        }}
        .filter-row .form-label {{
            margin: 0;
            font-size: 0.85rem;
            font-weight: 600;
            color: var(--yahoo-text-gray);
            white-space: nowrap;
        }}
        
        /* テーブルスタイル */
        .table-responsive {{
            background-color: white;
            border-radius: 0.5rem;
            overflow: hidden;
            width: 100%;
        }}
        .table {{
            margin-bottom: 0;
            border-collapse: separate;
            border-spacing: 0;
            width: 100%;
        }}
        .table tbody tr {{
            white-space: nowrap;
        }}
        .table tbody .stock-cell {{
            white-space: normal;
        }}
        .table thead th {{
            background-color: white;
            color: var(--yahoo-text-gray);
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            padding: 0.75rem 1rem;
            border-bottom: 1px solid var(--yahoo-border);
            border-top: none;
            border-left: none;
            border-right: none;
            vertical-align: middle;
        }}
        .table tbody td {{
            font-size: 0.9rem;
            padding: 0.75rem 1rem;
            vertical-align: middle;
            border-bottom: 1px solid var(--yahoo-border);
            border-left: none;
            border-right: none;
            border-top: none;
        }}
        .table tbody tr:hover {{
            background-color: rgba(0, 129, 241, 0.05);
        }}
        
        /* ティッカーコードと会社名 */
        .stock-info {{
            display: flex;
            align-items: center;
            gap: 0.5rem;
            flex-wrap: wrap;
            flex: 1;
        }}
        .ticker-code-link {{
            font-weight: 700;
            color: var(--yahoo-blue);
            text-decoration: none;
            font-size: 1rem;
            transition: color 0.2s;
        }}
        .ticker-code-link:hover {{
            color: #0066cc;
            text-decoration: underline;
        }}
        .company-name {{
            color: var(--yahoo-text-gray);
            font-size: 0.9rem;
        }}
        
        /* サイズバッジ（アウトラインスタイル） */
        .cap-badge {{
            display: inline-block;
            padding: 0.15rem 0.5rem;
            font-size: 0.7rem;
            font-weight: 600;
            border: 1px solid var(--yahoo-border);
            border-radius: 0.25rem;
            background-color: transparent;
            margin-right: 0.25rem;
        }}
        .cap-badge-large {{
            color: var(--yahoo-blue);
            border-color: var(--yahoo-blue);
        }}
        .cap-badge-medium {{
            color: var(--yahoo-text-gray);
            border-color: var(--yahoo-text-gray);
        }}
        .cap-badge-small {{
            color: var(--yahoo-text-gray);
            border-color: var(--yahoo-border);
        }}
        
        /* 数値セル */
        .numeric-cell {{
            font-weight: 600;
            text-align: right;
        }}
        
        /* 成長率/利益率セル */
        .growth-margin-cell {{
            font-weight: 600;
        }}
        .growth-positive {{
            color: var(--yahoo-green);
        }}
        .growth-negative {{
            color: #d32f2f;
        }}
        
        /* スコアバッジ */
        .score-badge {{
            display: inline-block;
            padding: 0.25rem 0.75rem;
            background-color: var(--yahoo-blue);
            color: white;
            border-radius: 0.25rem;
            font-weight: 600;
            font-size: 0.85rem;
        }}
        
        /* 詳細ボタン */
        .btn-details {{
            background-color: transparent;
            border: 1px solid var(--yahoo-border);
            color: var(--yahoo-text-gray);
            padding: 0.25rem 0.5rem;
            border-radius: 0.25rem;
            transition: all 0.2s;
        }}
        .btn-details:hover {{
            background-color: var(--yahoo-blue);
            border-color: var(--yahoo-blue);
            color: white;
        }}
        .btn-details svg {{
            transition: transform 0.2s;
        }}
        .btn-details.active svg {{
            transform: rotate(180deg);
        }}
        
        /* Yahoo Financeボタン */
        .btn-yahoo {{
            background-color: var(--yahoo-blue);
            color: white;
            border: none;
            padding: 0.25rem 0.75rem;
            border-radius: 0.25rem;
            font-size: 0.8rem;
            font-weight: 600;
            text-decoration: none;
            transition: background-color 0.2s;
        }}
        .btn-yahoo:hover {{
            background-color: #0066cc;
            color: white;
        }}
        
        /* スコア背景色は削除（不要な情報として削除） */
        
        /* 財務詳細行 */
        .financial-details-row {{
            background-color: #fafbfc;
        }}
        .financial-details-row td {{
            padding: 0 !important;
        }}
        .financial-details-row .table-sm {{
            margin-bottom: 0;
        }}
        .financial-details-row .table-sm td {{
            padding: 0.5rem;
        }}
        
        /* セクターフィルター */
        .sector-filter-select {{
            min-width: 200px;
            max-width: 300px;
            padding: 0.4rem 0.75rem;
            font-size: 0.85rem;
            border: 1px solid var(--yahoo-border);
            border-radius: 0.25rem;
            background-color: white;
            transition: border-color 0.15s ease-in-out;
        }}
        .sector-filter-select:focus {{
            border-color: var(--yahoo-blue);
            outline: 0;
            box-shadow: 0 0 0 0.2rem rgba(0, 129, 241, 0.15);
        }}
        
        /* タブナビゲーション */
        .nav-tabs {{
            border-bottom: 2px solid var(--yahoo-border);
        }}
        .nav-tabs .nav-link {{
            border: none;
            border-bottom: 3px solid transparent;
            color: var(--yahoo-text-gray);
            font-weight: 600;
            padding: 0.75rem 1.5rem;
            transition: all 0.2s;
        }}
        .nav-tabs .nav-link:hover {{
            border-bottom-color: rgba(0, 129, 241, 0.5);
            color: var(--yahoo-blue);
        }}
        .nav-tabs .nav-link.active {{
            color: var(--yahoo-blue);
            border-bottom-color: var(--yahoo-blue);
            background-color: transparent;
        }}
        
        /* タブコンテンツ */
        .tab-content {{
            background-color: white;
            border: 1px solid var(--yahoo-border);
            border-top: none;
            border-radius: 0 0 0.5rem 0.5rem;
            padding: 1.5rem;
        }}
        
        /* 銘柄セル */
        .stock-cell {{
            display: flex;
            align-items: center;
            gap: 0.5rem;
            flex-wrap: wrap;
        }}
        /* 詳細ボタン（ANALYZE） */
        .btn-analyze {{
            background-color: transparent;
            border: 1px solid var(--yahoo-blue);
            color: var(--yahoo-blue);
            padding: 0.2rem 0.6rem;
            border-radius: 0.25rem;
            font-size: 0.6rem;
            font-weight: 600;
            letter-spacing: 0.05em;
            cursor: pointer;
            transition: all 0.2s;
            flex-shrink: 0;
            text-transform: uppercase;
        }}
        .btn-analyze:hover {{
            background-color: var(--yahoo-blue);
            color: white;
        }}
        
        /* 指標セル */
        .metrics-value {{
            font-weight: 600;
            font-size: 0.95rem;
            margin-bottom: 0.2rem;
        }}
        .metrics-label {{
            font-size: 0.7rem;
            color: var(--yahoo-text-gray);
        }}
        
        /* 時価総額セル */
        .market-cap-value {{
            font-weight: 600;
            font-size: 0.95rem;
            margin-bottom: 0.2rem;
        }}
        .market-cap-label {{
            font-size: 0.7rem;
            color: var(--yahoo-text-gray);
        }}
        
        /* モーダルスタイル */
        .modal-summary-card {{
            background-color: #f8f9fa;
            border-radius: 0.5rem;
            padding: 1rem;
            margin-bottom: 1.5rem;
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 1rem;
        }}
        .summary-item {{
            display: flex;
            flex-direction: column;
            gap: 0.25rem;
        }}
        .summary-label {{
            font-size: 0.75rem;
            color: var(--yahoo-text-gray);
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }}
        .summary-value {{
            font-size: 1rem;
            font-weight: 600;
            color: var(--yahoo-navy);
        }}
        .modal-section-title {{
            font-weight: 600;
            color: var(--yahoo-navy);
            margin-bottom: 0.75rem;
            font-size: 1rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }}
        .modal-financial-scores {{
            margin-bottom: 1.5rem;
        }}
        .scores-grid {{
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 0.75rem;
        }}
        .score-item {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 0.5rem;
            background-color: #f8f9fa;
            border-radius: 0.25rem;
        }}
        .score-label {{
            font-size: 0.85rem;
            color: var(--yahoo-text-gray);
        }}
        .score-value {{
            font-size: 0.95rem;
            font-weight: 600;
            color: var(--yahoo-navy);
        }}
        .modal-financial-table {{
            margin-top: 1.5rem;
        }}
        .modal-financial-table .table-responsive {{
            max-height: 400px;
            overflow-y: auto;
        }}
        .ticker-code-large {{
            font-weight: 700;
            color: var(--yahoo-blue);
            font-size: 1.1rem;
            margin-right: 0.5rem;
        }}
        
        /* フッター */
        .main-footer {{
            background-color: var(--yahoo-navy);
            color: rgba(255, 255, 255, 0.7);
            padding: 1.5rem 2rem;
            margin-top: 3rem;
            border-top: 1px solid var(--yahoo-border);
        }}
        .footer-content {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 1rem;
        }}
        .footer-legend {{
            display: flex;
            gap: 1rem;
            flex-wrap: wrap;
        }}
        .footer-legend-item {{
            font-size: 0.85rem;
        }}
        .footer-legend-label {{
            font-weight: 600;
            margin-right: 0.5rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }}
        .footer-update-time {{
            font-size: 0.85rem;
        }}
        
        /* スマホ最適化 */
        @media (max-width: 768px) {{
            .main-content {{
                padding: 1rem;
            }}
            .header-content {{
                padding: 0.75rem 1rem;
                flex-direction: column;
                align-items: flex-start;
                gap: 0.5rem;
            }}
            .header-nav {{
                flex-direction: column;
                width: 100%;
            }}
            .header-nav .nav-link {{
                width: 100%;
                text-align: left;
                padding: 0.5rem 1rem;
            }}
            .filter-row {{
                flex-direction: column;
                align-items: flex-start;
                gap: 0.5rem;
            }}
            .sector-filter-select {{
                width: 100%;
                max-width: 100%;
            }}
            .table-responsive {{
                overflow-x: auto;
                -webkit-overflow-scrolling: touch;
            }}
            .table thead th {{
                font-size: 0.7rem;
                padding: 0.5rem 0.75rem;
            }}
            .table tbody td {{
                font-size: 0.85rem;
                padding: 0.5rem 0.75rem;
            }}
            .stock-cell {{
                flex-direction: column;
                align-items: flex-start;
                gap: 0.5rem;
            }}
            .stock-info {{
                width: 100%;
            }}
            .ticker-code-link {{
                font-size: 0.9rem;
            }}
            .btn-analyze {{
                align-self: flex-end;
                margin-top: 0.25rem;
            }}
            .tab-content {{
                padding: 1rem;
            }}
            .modal-dialog {{
                margin: 0.5rem;
            }}
            .modal-content {{
                border-radius: 0.5rem;
            }}
            .modal-body {{
                padding: 1rem;
            }}
            .modal-financial-section .row {{
                flex-direction: column;
            }}
            .modal-financial-section .col-md-4 {{
                width: 100%;
                margin-bottom: 1rem;
            }}
        }}
        
        @media (max-width: 480px) {{
            .header-title {{
                font-size: 1rem;
            }}
            .table thead th {{
                font-size: 0.65rem;
                padding: 0.4rem 0.5rem;
            }}
            .table tbody td {{
                font-size: 0.8rem;
                padding: 0.4rem 0.5rem;
            }}
            .ticker-code-link {{
                font-size: 0.85rem;
            }}
            .legend-row {{
                font-size: 0.75rem;
            }}
            .legend-row span {{
                display: block;
                margin-bottom: 0.25rem;
            }}
        }}
    </style>
</head>
<body class="bg-light">
    <!-- ヘッダー -->
    <header class="main-header">
        <div class="header-content">
            <div class="header-title">J-Equity</div>
            <div class="header-strategy" id="header-strategy">Value</div>
        </div>
    </header>
    
    <div class="container-fluid mt-4">
        <ul class="nav nav-tabs mb-3" id="rankingTabs" role="tablist">
            <li class="nav-item" role="presentation">
                <button class="nav-link active" id="value-tab" data-bs-toggle="tab" data-bs-target="#value-pane" type="button" role="tab" aria-controls="value-pane" aria-selected="true" onclick="updateHeaderStrategy('Value')">
                    💎 Value Strategy
                </button>
            </li>
            <li class="nav-item" role="presentation">
                <button class="nav-link" id="growth-tab" data-bs-toggle="tab" data-bs-target="#growth-pane" type="button" role="tab" aria-controls="growth-pane" aria-selected="false" onclick="updateHeaderStrategy('Growth')">
                    🚀 Growth Strategy
                </button>
            </li>
        </ul>

        <div class="tab-content" id="rankingTabsContent">
            <div class="tab-pane fade show active" id="value-pane" role="tabpanel" aria-labelledby="value-tab">
                <!-- フィルター -->
                <div class="filter-row">
                    <label for="value-sector-filter" class="form-label">業種で絞り込む: </label>
                    {value_sector_select}
                </div>
                <div class="table-responsive">
                    <table class="table table-hover" id="value-table">
                        <thead>
                            <tr>
                                <th class="stock-header">銘柄</th>
                                <th class="metrics-header">主要指標</th>
                                <th class="market-cap-header">時価総額<br><small>(億円)</small></th>
                            </tr>
                        </thead>
                        <tbody>
{value_rows}
                        </tbody>
                    </table>
                </div>
                {value_modals}
            </div>
            
            <div class="tab-pane fade" id="growth-pane" role="tabpanel" aria-labelledby="growth-tab">
                <!-- フィルター -->
                <div class="filter-row">
                    <label for="growth-sector-filter" class="form-label">業種で絞り込む: </label>
                    {growth_sector_select}
                </div>
                <div class="table-responsive">
                    <table class="table table-hover" id="growth-table">
                        <thead>
                            <tr>
                                <th class="stock-header">銘柄</th>
                                <th class="metrics-header">主要指標</th>
                                <th class="market-cap-header">時価総額<br><small>(億円)</small></th>
                            </tr>
                        </thead>
                        <tbody>
{growth_rows}
                        </tbody>
                    </table>
                </div>
                {growth_modals}
            </div>
        </div>

    </div>
    
    <!-- フッター -->
    <footer class="main-footer">
        <div class="footer-content">
            <div class="footer-legend">
                <span class="footer-legend-label">MARK LEGEND</span>
                <span class="footer-legend-item">🔥 Growth</span>
                <span class="footer-legend-item">💎 Debt-Free</span>
                <span class="footer-legend-item">💰 Net Cash</span>
                <span class="footer-legend-item">⚠️ Data Missing</span>
            </div>
            <div class="footer-update-time">
                更新日時: {update_time_jst}
            </div>
        </div>
    </footer>
    
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    <script>
        function showModal(modalId) {{
            const modalElement = document.getElementById(modalId);
            if (modalElement) {{
                const modal = new bootstrap.Modal(modalElement);
                modal.show();
            }} else {{
                console.warn('モーダルが見つかりません: ' + modalId);
            }}
        }}
        
        function updateHeaderStrategy(strategy) {{
            const headerStrategy = document.getElementById('header-strategy');
            if (headerStrategy) {{
                headerStrategy.textContent = strategy;
                headerStrategy.className = 'header-strategy ' + strategy.toLowerCase();
            }}
        }}
        
        function filterBySector(tableId, selectId, prefix) {{
            const select = document.getElementById(selectId);
            const table = document.getElementById(tableId);
            const selectedSector = select.value;
            // stock-rowクラスの行のみを取得
            const rows = table.querySelectorAll('tbody tr.stock-row');
            
            rows.forEach(row => {{
                // data-sector属性から業種を取得
                const sector = row.getAttribute('data-sector');
                
                if (selectedSector === 'all' || sector === selectedSector) {{
                    // 銘柄行を表示
                    row.style.display = '';
                }} else {{
                    // 銘柄行を非表示
                    row.style.display = 'none';
                }}
            }});
        }}
        
        // ページ読み込み時にイベントリスナーを設定
        document.addEventListener('DOMContentLoaded', function() {{
            const valueFilter = document.getElementById('value-sector-filter');
            if (valueFilter) {{
                valueFilter.addEventListener('change', function() {{
                    filterBySector('value-table', 'value-sector-filter', 'value');
                }});
            }}
            
            const growthFilter = document.getElementById('growth-sector-filter');
            if (growthFilter) {{
                growthFilter.addEventListener('change', function() {{
                    filterBySector('growth-table', 'growth-sector-filter', 'growth');
                }});
            }}
        }});
    </script>
</body>
</html>
        """
        return html


    def generate_report(self) -> str:
        """
        レポートを生成して保存
        
        Returns:
            保存されたファイルパス
        """
        # バリュー株推奨リストを読み込み
        value_csv_path = self.processed_data_dir / "value_recommendations.csv"
        value_df = pd.DataFrame()
        if value_csv_path.exists():
            try:
                value_df = pd.read_csv(value_csv_path, encoding='utf-8-sig')
                logger.info(f"バリュー株データ読み込み完了: {len(value_df)}銘柄")
            except Exception as e:
                logger.error(f"バリュー株CSV読み込みエラー: {str(e)}")
        else:
            logger.warning(f"バリュー株ファイルが見つかりません: {value_csv_path}")
        
        # グロース株推奨リストを読み込み
        growth_csv_path = self.processed_data_dir / "growth_recommendations.csv"
        growth_df = pd.DataFrame()
        if growth_csv_path.exists():
            try:
                growth_df = pd.read_csv(growth_csv_path, encoding='utf-8-sig')
                logger.info(f"グロース株データ読み込み完了: {len(growth_df)}銘柄")
            except Exception as e:
                logger.error(f"グロース株CSV読み込みエラー: {str(e)}")
        else:
            logger.warning(f"グロース株ファイルが見つかりません: {growth_csv_path}")
        
        if value_df.empty and growth_df.empty:
            logger.error("読み込めるデータがありません")
            return ""
        
        # HTMLを生成
        html = self.generate_html(value_df, growth_df)
        
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
