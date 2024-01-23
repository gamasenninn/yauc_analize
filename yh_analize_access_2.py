import pandas as pd
import os
import glob
import datetime
import pyodbc
import gc
from dotenv import load_dotenv
import re
import sys

# 環境変数からディレクトリのパスを設定
load_dotenv()

WORK_DATA_DIR = os.environ["WORK_DATA_DIR"]
WORK_GRAPH_DIR = os.environ["WORK_GRAPH_DIR"]
WORK_LASTSUM_DIR = os.environ["WORK_LASTSUM_DIR"]
CONN_STR = os.environ["CONN_STR"]

def get_data_files(directory):
    """指定されたディレクトリからCSVファイルのリストを取得し、ソートする。"""
    file_list = glob.glob(f'{directory}/YBIZ_c_*.csv')
    file_list.sort()
    return file_list

def load_and_process_file(file_path):
    """ファイルを読み込み、必要な処理を行う。"""
    date = re.search(r'[0-9]{6}', file_path).group()
    df = pd.read_csv(file_path, encoding="cp932")
    df['日付'] = date
    for col in ['アクセス増分', 'アクセス累計', 'ウォッチ増分', 'ウォッチ累計', 'MAアクセス増分', 'MAウォッチ増分', 'データ数']:
        df[col] = 0
    return df

def calculate_metrics(df):
    """データフレームに対して、アクセス数とウォッチ数の増分、累計、移動平均を計算する。"""
    df = df.sort_values('日付').reset_index(drop=True)

    # アクセス数とウォッチ数の初期値を設定
    df['アクセス増分'] = df['アクセス数'].diff().fillna(0).clip(lower=0)
    df['ウォッチ増分'] = df['ウォッチ数'].diff().fillna(0).clip(lower=0)

    # 累計値の計算
    df['アクセス累計'] = df['アクセス増分'].cumsum()
    df['ウォッチ累計'] = df['ウォッチ増分'].cumsum()

    # 移動平均の計算 (3日間)
    df['MAアクセス増分'] = df['アクセス増分'].rolling(window=3, min_periods=1).mean()
    df['MAウォッチ増分'] = df['ウォッチ増分'].rolling(window=3, min_periods=1).mean()

    # データ数の計算
    df['データ数'] = range(1, len(df) + 1)

    return df

def main():
    data_files = get_data_files(WORK_DATA_DIR)
    if not data_files:
        print("解析対象のデータがありません")
        sys.exit(0)

    sum_df = pd.concat([load_and_process_file(f) for f in data_files])

    k_set = set(sum_df['管理番号'])
    cnxn = pyodbc.connect(CONN_STR)
    p_df = pd.read_sql(sql="SELECT * FROM 商品マスタ where 在庫数量>0 ", con=cnxn)
    k_set &= set(p_df['コード'])

    # ... 以前のコード ...

    # 管理番号のセットを用いて、在庫がある商品のみをフィルタリング
    filtered_df = sum_df[sum_df['管理番号'].isin(k_set)]

    # 最終的なデータフレーム
    last_df = pd.DataFrame()

    # 各管理番号に対して処理を実行
    for k in k_set:
        # 特定の管理番号に対するデータを選択
        k_df = filtered_df[filtered_df['管理番号'] == k].copy()

        # 必要な計算を実施
        k_df = calculate_metrics(k_df)

        # 各管理番号の最終行のみをlast_dfに追加
        last_df = last_df.append(k_df.iloc[-1])

        # オプション: 管理番号ごとのデータをCSVファイルに保存
        try:
            k_df.to_csv(os.path.join(WORK_GRAPH_DIR, f'anl_{k}.csv'), encoding='cp932')
        except Exception as e:
            print(f"ファイル保存エラー: {k}, エラー: {e}")

    # last_dfに含まれる最終集計データをCSVファイルに保存
    last_df.to_csv(os.path.join(WORK_LASTSUM_DIR, f'last_sum_{datetime.date.today():%y%m%d}.csv'), encoding='cp932')


if __name__ == "__main__":
    main()
