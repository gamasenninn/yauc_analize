# coding: UTF-8

import pandas as pd
import sys
import re
import datetime
import os
import glob
import pyodbc
from dotenv import load_dotenv

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

def main():

    # 解析対象の処理リストを作る
    data_files = get_data_files(WORK_DATA_DIR)
    if not data_files:
        print("解析対象のデータがありません")
        sys.exit(0)

    sum_df = pd.concat([load_and_process_file(f) for f in data_files])


    k_set = set(sum_df['管理番号'])
    # データベースから在庫ありのものを抽出する
    cnxn = pyodbc.connect(CONN_STR)
    p_df = pd.read_sql(sql="SELECT * FROM 商品マスタ where 在庫数量>0 ", con=cnxn)
    k_set = k_set & set(p_df['コード'])
    #print(k_set)
    #----------------------------------
    last_df = pd.DataFrame()
    k_l = list(k_set)
    for k in k_l:
        k_df = sum_df[sum_df['管理番号']==k].copy()
        #k_df = sum_df[sum_df['管理番号']==k]
        k_df.sort_values('日付')
        k_df.reset_index(inplace=True)
    #    print (k_df)
        o_ac = int(k_df.iloc[0]['アクセス数'])
        o_wc = int(k_df.iloc[0]['ウォッチ数'])
        sum_ac = o_ac
        sum_wc = o_wc
        last_idx =0
        for idx,row in k_df.iterrows():
            n_ac = int(row['アクセス数'])
            n_wc = int(row['ウォッチ数'])
            #---アクセス正規化----
            dt_ac =n_ac - o_ac
            if dt_ac < 0 :
                dt_ac = n_ac
            k_df.at[idx,'アクセス増分'] = dt_ac
            sum_ac += dt_ac
            k_df.at[idx,'アクセス累計'] += sum_ac
            o_ac = n_ac
            #---ウォッチ正規化----
            dt_wc =n_wc - o_wc
            #if dt_wc < 0 :
            #    dt_wc = n_wc
            k_df.at[idx,'ウォッチ増分'] = dt_wc
            sum_wc += dt_wc
            k_df.at[idx,'ウォッチ累計'] += sum_wc
            o_wc = n_wc
            k_df.at[idx,'データ数'] = idx+1
            last_idx = idx

        k_df['MAアクセス増分'] = k_df['アクセス増分'].rolling(3).mean()
        k_df['MAウォッチ増分'] = k_df['ウォッチ増分'].rolling(3).mean()
        last_df = last_df.append(k_df.loc[last_idx])

        print(k_df.tail(1))    

        try:
            k_df.to_csv(f'{WORK_GRAPH_DIR}/anl_'+k+'.csv', encoding='cp932')
        except Exception as e:
            print(f"ファイル保存エラー: {k}, エラー: {e}")


    last_df.to_csv(f'{WORK_LASTSUM_DIR}/last_sum_'+str(datetime.date.today())+'.csv',encoding='cp932')


if __name__ == "__main__":
    main()

