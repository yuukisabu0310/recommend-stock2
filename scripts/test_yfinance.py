"""yfinanceで日本株の財務データが取得できるかテスト"""
import yfinance as yf
import time
import pandas as pd

ticker = "7203.T"
print(f"テスト銘柄: {ticker}")
print("=" * 60)

stock = yf.Ticker(ticker)

# 待機時間（レート制限対策）
print("レート制限対策のため10秒待機中...")
time.sleep(10)

# 基本情報
print("\n1. 基本情報")
try:
    info = stock.info
    if info:
        print(f"  銘柄名: {info.get('longName', 'N/A')}")
        print(f"  セクター: {info.get('sector', 'N/A')}")
    else:
        print("  情報なし")
except Exception as e:
    print(f"  エラー: {e}")

# 財務データ（年次）
print("\n2. 財務データ（年次）")
try:
    financials = stock.financials
    if financials is not None and not financials.empty:
        print(f"  形状: {financials.shape}")
        print(f"  カラム数: {len(financials.columns)}")
        print(f"  カラム: {list(financials.columns)}")
        print(f"  インデックス（最初の10個）: {list(financials.index[:10])}")
    else:
        print("  データが空です")
except Exception as e:
    print(f"  エラー: {e}")

# 四半期財務データ
print("\n3. 四半期財務データ")
try:
    quarterly_financials = stock.quarterly_financials
    if quarterly_financials is not None and not quarterly_financials.empty:
        print(f"  形状: {quarterly_financials.shape}")
        print(f"  カラム数: {len(quarterly_financials.columns)}")
    else:
        print("  データが空です")
except Exception as e:
    print(f"  エラー: {e}")

# キャッシュフロー
print("\n4. キャッシュフロー")
try:
    cashflow = stock.cashflow
    if cashflow is not None and not cashflow.empty:
        print(f"  形状: {cashflow.shape}")
        print(f"  カラム数: {len(cashflow.columns)}")
    else:
        print("  データが空です")
except Exception as e:
    print(f"  エラー: {e}")

# 貸借対照表
print("\n5. 貸借対照表")
try:
    balance_sheet = stock.balance_sheet
    if balance_sheet is not None and not balance_sheet.empty:
        print(f"  形状: {balance_sheet.shape}")
        print(f"  カラム数: {len(balance_sheet.columns)}")
    else:
        print("  データが空です")
except Exception as e:
    print(f"  エラー: {e}")

# 四半期キャッシュフロー
print("\n6. 四半期キャッシュフロー")
try:
    quarterly_cashflow = stock.quarterly_cashflow
    if quarterly_cashflow is not None and not quarterly_cashflow.empty:
        print(f"  形状: {quarterly_cashflow.shape}")
    else:
        print("  データが空です")
except Exception as e:
    print(f"  エラー: {e}")

# 四半期貸借対照表
print("\n7. 四半期貸借対照表")
try:
    quarterly_balance_sheet = stock.quarterly_balance_sheet
    if quarterly_balance_sheet is not None and not quarterly_balance_sheet.empty:
        print(f"  形状: {quarterly_balance_sheet.shape}")
    else:
        print("  データが空です")
except Exception as e:
    print(f"  エラー: {e}")

print("\n" + "=" * 60)
