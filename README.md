# recommend-stock2
日本株 成長×割安スクリーニングシステム

## 概要
日本株（主に東証プライム）を対象に、「成長しており」「質が高く」「今は割安」な銘柄を抽出するシステムです。

## プロジェクト構造
```
recommend-stock2/
├── .github/
│   └── workflows/
│       └── daily_scan.yml    # GitHub Actions自動実行設定
├── scripts/
│   ├── fetcher.py            # 財務データ取得モジュール
│   ├── analyzer.py           # 財務データ分析・スコアリング
│   ├── reporter.py           # HTMLレポート生成
│   ├── test_yfinance.py      # yfinanceテスト用スクリプト
│   └── ticker_list.py        # 銘柄リスト管理（現在は未使用）
├── data/
│   ├── raw/                  # 生データ（JSON形式で個別保存）
│   │   ├── {ticker}.json     # 各銘柄の財務データ（JSON形式）
│   │   └── jpx_tse_info.csv  # 東証銘柄情報マスターファイル
│   ├── processed/            # 処理済みデータ
│   │   ├── final_recommendations.csv  # 最終推奨銘柄リスト
│   │   └── screened_results.csv       # スクリーニング結果
│   └── logs/                 # ログファイル
├── docs/
│   └── index.html            # GitHub Pages用HTMLレポート
├── config.yaml               # 設定ファイル（閾値など）
├── constants.py              # 定数定義（業種分類など）
└── requirements.txt          # 依存パッケージ
```

## セットアップ

### 1. 依存パッケージのインストール
```bash
pip install -r requirements.txt
```

### 2. 設定ファイルの確認
`config.yaml`で各種閾値を調整できます。

### 3. 銘柄マスターファイルの準備
`data/raw/jpx_tse_info.csv`に東証銘柄情報を配置してください。
このファイルから全銘柄リストを自動的に読み込みます。

## 使用方法

### Step 1: 財務データの取得

インクリメンタル取得モード（推奨）を使用します。
処理済み銘柄を自動的にスキップし、未処理銘柄のみを取得します。

```python
from scripts.fetcher import IncrementalStockDataFetcher

# フェッチャーを初期化（最大500銘柄まで）
fetcher = IncrementalStockDataFetcher(max_tickers_per_run=500)

# インクリメンタル取得を実行
fetched_count = fetcher.fetch_incremental()
```

コマンドラインから実行する場合：
```bash
python scripts/fetcher.py
```

**インクリメンタル取得の特徴**:
- `jpx_tse_info.csv`から全銘柄を読み込み
- 既に取得済みの銘柄（`data/raw/{ticker}.json`が存在）は自動的にスキップ
- 未処理銘柄を優先して処理
- 1回の実行で最大500銘柄まで取得（`max_tickers_per_run`で調整可能）
- レート制限対策のため、銘柄ごとにランダムな待機時間を設定

### Step 2: 財務データの分析とスコアリング

取得したデータを分析し、スコアリングを行います。

```python
from scripts.analyzer import FinancialDataAnalyzer

# アナライザーを初期化
analyzer = FinancialDataAnalyzer()

# 全銘柄を分析
results_df = analyzer.analyze_all()

# 結果を保存
analyzer.save_results(results_df)

# 最終推奨リストを生成
final_df = analyzer.generate_final_recommendations(results_df)
analyzer.save_final_recommendations(final_df)
```

コマンドラインから実行する場合：
```bash
python scripts/analyzer.py
```

**スコアリングシステム**:
- **成長スコア（40点）**: 売上成長率に基づく評価
- **収益性スコア（30点）**: ROE（自己資本利益率）に基づく評価
- **割安度スコア（20点）**: PBR/PERに基づく評価
- **安全性スコア（10点）**: 自己資本比率に基づく評価
- **ペナルティ**: 営業利益または売上成長率がマイナスの場合、-40点
- **総合スコア**: 100点満点（Sランク: 80点以上、Aランク: 60点以上、Bランク: 40点以上、Cランク: 40点未満）

### Step 3: HTMLレポートの生成

分析結果をGitHub Pages用のHTMLレポートとして生成します。

```python
from scripts.reporter import ReportGenerator

# レポートジェネレーターを初期化
generator = ReportGenerator()

# レポートを生成
filepath = generator.generate_report()
```

コマンドラインから実行する場合：
```bash
python scripts/reporter.py
```

**レポート機能**:
- Bootstrap 5を使用したモダンなUI
- タブ切り替え機能（総合ランキング / 成長株 / 割安株）
- スコアの視覚化（プログレスバー、アイコン）
- 財務実値の表示（売上高、営業利益、PER、PBRなど）
- ツールチップによる詳細情報表示

## データ形式

### 生データ（`data/raw/`）
各銘柄の財務データはJSON形式で個別保存されます：
- `{ticker}.json`: 各銘柄の財務データ（例: `7203.json`）
  - `financials`: 損益計算書データ
  - `cashflow`: キャッシュフローデータ
  - `balance_sheet`: 貸借対照表データ
  - `info`: 銘柄基本情報（ROE、PBR、PERなど）

### 処理済みデータ（`data/processed/`）
- `final_recommendations.csv`: 最終推奨銘柄リスト（総合スコア順）
- `screened_results.csv`: 全銘柄のスクリーニング結果

## GitHub Actions による自動実行

### 毎日の自動スキャン
`.github/workflows/daily_scan.yml` により、毎日日本時間 08:00（UTC 23:00）に自動実行されます。

**実行フロー**:
1. **データ取得**: `scripts/fetcher.py`を実行し、未処理銘柄のデータを取得
2. **データ分析**: `scripts/analyzer.py`を実行し、スコアリングを実施
3. **レポート生成**: `scripts/reporter.py`を実行し、HTMLレポートを生成
4. **自動コミット**: 取得したデータとレポートを自動的にコミット・プッシュ

**機能**:
- `actions/cache` を使用して `data/raw` フォルダを保存・復元
- 未処理の銘柄を自動的に抽出（1回あたり最大500銘柄）
- 取得できたデータを自動的にコミット・プッシュ
- エラーが発生してもキャッシュを保存（データ損失を防止）

**手動実行**:
GitHubのActionsタブから「Daily Stock Data Scan」ワークフローを手動実行することも可能です。

## GitHub Pages

`docs/index.html`が自動的にGitHub Pagesで公開されます。
レポートは以下の3つのタブで表示されます：
- **総合ランキング**: 総合スコア順のランキング
- **成長株**: 成長スコア + 収益性スコア順のランキング
- **割安株**: 割安度スコア + 安全性スコア順のランキング

## 注意事項

- yfinanceを使用してデータを取得します
- レート制限対策のため、銘柄ごとに待機時間が設けられています（0.8〜1.5秒のランダム待機）
- 404エラーが5回連続で発生した場合、取得処理を中断します
- 欠損値がある場合はログに記録されます
- 全銘柄リストは `data/raw/jpx_tse_info.csv` から自動的に読み込まれます

## ライセンス

このプロジェクトはMITライセンスの下で公開されています。
