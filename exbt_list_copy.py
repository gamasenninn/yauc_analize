import os
import pandas as pd
import datetime
from dotenv import load_dotenv

# 環境変数をロード
load_dotenv()

# 環境変数からディレクトリのパスを設定
exb_list_dir = os.environ["EXB_LIST_DIR"]
work_data_dir = os.environ["WORK_DATA_DIR"]

def load_dataframe(directory, filename, encoding):
    """
    指定されたディレクトリとファイル名からデータフレームを読み込む。

    Parameters:
    directory (str): ファイルが存在するディレクトリのパス
    filename (str): 読み込むファイルの名前
    encoding (str): ファイルのエンコーディング

    Returns:
    DataFrame: 読み込んだデータを含むPandasデータフレーム
    """
    file_path = os.path.join(directory, filename)
    return pd.read_csv(file_path, encoding=encoding)

def save_dataframe(dataframe, directory, prefix, date, encoding):
    """
    データフレームをCSVファイルとして保存する。

    Parameters:
    dataframe (DataFrame): 保存するデータフレーム
    directory (str): CSVファイルを保存するディレクトリのパス
    prefix (str): 生成されるファイル名のプレフィックス
    date (str): ファイル名に含める日付
    encoding (str): ファイルのエンコーディング
    """
    filename = os.path.join(directory, f'{work_data_dir}/{prefix}_{date}.csv')
    dataframe.to_csv(filename, encoding=encoding)

def list_copy():
    """
    主処理関数。CSVファイルを読み込み、指定されたカラムでデータフレームを作成し、
    新しいCSVファイルとして保存する。
    """
    cur_dir = os.path.dirname(os.path.abspath(__file__))

    today_str = datetime.date.today().strftime('%y%m%d')
    target_filename = f'{today_str}_exbt_list.csv'
    encoding_type = 'cp932'  # または必要に応じて 'utf-8'

    target_df = load_dataframe(exb_list_dir, target_filename, encoding_type)

    collist = ["管理番号", "YID", "タイトル", "現在価格", "即決価格", "アクセス数", "ウォッチ数"]
    df = target_df[['scode', 'auc_id', 'title', 'start_price', 'bid_price', 'pv', 'watch']]
    df.columns = collist

    save_dataframe(df, cur_dir, 'YBIZ_u', today_str, 'utf-8')
    save_dataframe(df, cur_dir, 'YBIZ_c', today_str, encoding_type)

if __name__ == "__main__":
    list_copy()


