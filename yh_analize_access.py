# coding: UTF-8

import pandas as pd
import sys
import re
import datetime
import os
import glob
import matplotlib as mpl
#import matplotlib
#import matplotlib.pyplot as plt
import pyodbc
import gc
from dotenv import load_dotenv

# 環境変数からディレクトリのパスを設定
load_dotenv()

WORK_DATA_DIR = os.environ["WORK_DATA_DIR"]
WORK_GRAPH_DIR = os.environ["WORK_GRAPH_DIR"]
WORK_LASTSUM_DIR = os.environ["WORK_LASTSUM_DIR"]
CONN_STR = os.environ["CONN_STR"]

# プロット設定
#plt.style.use('ggplot') 
#font = {'family' : 'meiryo'}
#matplotlib.rc('font', **font)

# 解析対象の処理リストを作る
d_list = glob.glob(f'{WORK_DATA_DIR}/YBIZ_c_*.csv')
d_list.sort()

if not d_list:
    print("解析対象のデータがありません")
    sys.exit(0)


sum_df = pd.DataFrame()
for d in d_list:
    #print(d)
    dt = re.search('[0-9\-]+',d).group()
    df = pd.read_csv(d,encoding="cp932")
#    df = pd.read_csv(d)
    df['日付'] = dt
    df['アクセス増分'] = 0
    df['アクセス累計'] = 0
#    df['アクセス数'] = df['アクセス数']
    df['ウォッチ増分'] = 0
    df['ウォッチ累計'] = 0
#    df['ウォッチ数'] = df['ウォッチ数'])
    df['MAアクセス増分'] = 0
    df['MAウォッチ増分'] = 0
    df['データ数'] = 0
    sum_df = sum_df.append(df)

#sum_df['データ数'] = 0

del df

last_df = pd.DataFrame()
k_set = set(sum_df['管理番号'])

# データベースから在庫ありのものを抽出する
cnxn = pyodbc.connect(CONN_STR)

p_df = pd.read_sql(sql="SELECT * FROM 商品マスタ where 在庫数量>0 ", con=cnxn)

k_set = k_set & set(p_df['コード'])

del p_df

#print(k_set)

#----------------------------------

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

    #print(k_df.tail(1))    

    try:
        k_df.to_csv(f'{WORK_GRAPH_DIR}/anl_'+k+'.csv', encoding='cp932')
    except Exception as e:
        print("*************plotエラー:******************",k,e)

    del k_df
    gc.collect()


last_df.to_csv(f'{WORK_LASTSUM_DIR}/last_sum_'+str(datetime.date.today())+'.csv',encoding='cp932')

