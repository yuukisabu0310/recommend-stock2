# recommend-stock2
日本株 成長×割安スクリーニングシステム

## 概要
日本株（主に東証プライム）を対象に、「成長しており」「質が高く」「今は割安」な銘柄を抽出するシステムです。

## プロジェクト構造
```
recommend-stock2/
├── data/
│   ├── raw/          # 生データ（財務データ）
│   ├── processed/    # 処理済みデータ
│   └── logs/         # ログファイル
├── config.yaml       # 設定ファイル（閾値など）
├── constants.py      # 定数定義（業種分類など）
├── fetcher.py        # 財務データ取得モジュール
└── requirements.txt  # 依存パッケージ
```

## セットアップ

### 1. 依存パッケージのインストール
```bash
pip install -r requirements.txt
```

### 2. 設定ファイルの確認
`config.yaml`で各種閾値を調整できます。

## 使用方法

### Step 1: 財務データの取得

#### 分割取得モード（推奨）
処理済み銘柄を自動的にスキップし、未処理銘柄のみを取得します。

```python
from fetcher import StockDataFetcher
from ticker_list import get_all_tickers

# フェッチャーを初期化
fetcher = StockDataFetcher()

# 全銘柄リストから未処理銘柄を優先して取得（バッチサイズ制限付き）
all_tickers = get_all_tickers()
filepath = fetcher.fetch_incremental(all_tickers, sector="daily")
```

#### 直接指定モード
指定した銘柄リストをそのまま取得します（処理済みも含む）。

```python
from fetcher import StockDataFetcher

# フェッチャーを初期化
fetcher = StockDataFetcher()

# 単一銘柄の取得
data = fetcher.fetch_stock("7203")  # トヨタ自動車

# 複数銘柄の取得と保存（処理済みをスキップ）
tickers = ["7203", "6758", "9984"]
filepath = fetcher.fetch_and_save(tickers, sector="test", skip_processed=True)

# 処理済みも含めて取得
filepath = fetcher.fetch_and_save(tickers, sector="test", skip_processed=False)
```

### テスト実行

```bash
# 分割取得モード（デフォルト、推奨）
python fetcher.py incremental

# 直接指定モード
python fetcher.py direct
```

**分割取得の特徴**:
- 実行ごとに最大50〜100銘柄程度に制限（`config.yaml`で設定可能）
- すでにデータを取得済みの銘柄は自動的にスキップ
- まだ取得していない銘柄を優先して処理
- 数日かけて全銘柄を網羅する設計

## データ形式
取得したデータは以下の形式で`data/raw/`に保存されます：
- `{sector}_meta.csv`: メタデータ（銘柄情報、データ品質）
- `{sector}_PL.csv`: 損益計算書データ
- `{sector}_CF.csv`: キャッシュフローデータ
- `{sector}_BS.csv`: 貸借対照表データ

## GitHub Actions による自動実行

### 毎日の自動スキャン
`.github/workflows/daily_scan.yml` により、毎日日本時間 08:00 に自動実行されます。

**機能**:
- `actions/cache` を使用して `data/raw` フォルダを保存・復元
- 未処理の銘柄を自動的に抽出（1日あたり50銘柄まで）
- 取得できたデータを自動的にコミット・プッシュ
- 数日かけて全銘柄を網羅する設計

**手動実行**:
GitHubのActionsタブから「Daily Stock Data Scan」ワークフローを手動実行することも可能です。

## 注意事項
- yfinanceを使用してデータを取得します
- レート制限対策のため、銘柄ごとに待機時間が設けられています
- 欠損値がある場合はログに記録されます
- 全銘柄リストは `ticker_list.py` で管理されます（実際の運用では東証プライム全銘柄リストが必要）
