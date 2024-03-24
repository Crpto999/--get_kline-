import os
import shutil
import sys
from glob import glob
import pandas as pd
from config import *
from datetime import timedelta

target = 'spot'
mode = '现货'
# 检查是否有足够的命令行参数
if len(sys.argv) > 1:
    target = sys.argv[1]
print(f"————————————————————————————————开始更新 {target}数据至K线数据库")
if target == "spot":
    os.makedirs(现货K线存放路径, exist_ok=True)
    orginal_csv_path = 现货K线存放路径
    mode = '现货'
if target == "swap":
    orginal_csv_path = 永续合约K线存放路径
    os.makedirs(永续合约K线存放路径, exist_ok=True)
    mode = '合约'
new_csv_path = 下载文件夹

csv_files = glob(os.path.join(new_csv_path, '*.csv'))

for new_csv in csv_files:
    coin_name = os.path.basename(new_csv).split('_')[0]
    # print(f"正在处理 {coin_name} 的K线数据")

    if any(keyword in coin_name for keyword in ['UP', 'DOWN', 'BEAR', 'BULL']):
        print(f"{coin_name} 是用不到的K线数据，跳过")
        continue
    coin_name = coin_name.split('USDT')[0] + '-' + 'USDT'
    orginal_csv = os.path.join(orginal_csv_path, coin_name + '.csv')
    new_df = pd.read_csv(new_csv)
    # 增量更新（通过是否存在原始CSV数据判定）
    if os.path.exists(orginal_csv):
        original_df = pd.read_csv(orginal_csv, skiprows=1, encoding='gbk')

        # 转换为 datetime
        original_df['candle_begin_time'] = pd.to_datetime(original_df['candle_begin_time'])
        new_df['candle_begin_time'] = pd.to_datetime(new_df['candle_begin_time'])

        # 获取截止日期
        start_date_new_df = new_df['candle_begin_time'].iloc[0]

        original_df = original_df[original_df['candle_begin_time'] < start_date_new_df]

        # 拼接数据
        concatenated_df = pd.concat([original_df, new_df], ignore_index=True)
        concatenated_df.sort_values('candle_begin_time', inplace=True)
    # 首次下载,没有原始CSV数据
    else:
        concatenated_df = new_df

    # 生成新的文件，如有旧的，会覆盖旧的
    special_string = "本数据由喜顺有限公司整理"
    with open(orginal_csv, 'w', encoding='gbk', newline='') as file:
        file.write(special_string + '\n')
        concatenated_df.to_csv(file, index=False)
        print(f"{coin_name} {mode}数据 已更新至最新")
print(f"所有 {mode}数据 已更新至最新")
shutil.rmtree(new_csv_path)
print(f"下载临时文件夹 {new_csv_path} 已被删除")
