# -*- coding: utf-8 -*-
import os
import shutil
import sys
import time
import zipfile
from glob import glob
from itertools import product
from pathlib import Path
import pandas as pd
from joblib import Parallel, delayed
from tqdm import tqdm
from config import *

pd.set_option('display.max_rows', 1000)
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行


# 通过所有zip文件的文件名前缀获取币种名称列表
def extract_coin_names(folder_path):
    zip_files = glob(os.path.join(folder_path, '*.zip'))
    print('发现 {} 个zip 文件.'.format(len(zip_files)))

    coin_names = set()  # 使用集合来避免重复的币种名称
    for zip_file in zip_files:
        coin_name = os.path.basename(zip_file).split('-')[0]
        coin_names.add(coin_name)
    coin_names_list = list(coin_names)
    coin_names_list.sort()
    return coin_names_list


def all_merge_csv(folder_path):
    matching_files = list(Path(folder_path).glob(f"*{'_merge'}*.csv"))
    merge_files = set()  # 使用集合来避免重复的币种名称

    # 统计已存在多少个 _merge 的文件
    existing_merge_files_count = len(matching_files)
    if existing_merge_files_count > 2:
        print(f"检查到上次有任务中断。上次已完成 {existing_merge_files_count} 个币种的清洗任务，开始续洗.")

        for merge_file in matching_files:
            coin_name = os.path.basename(merge_file).split('_')[0]
            merge_files.add(coin_name)

        merge_files_list = list(merge_files)
        merge_files_list.sort()

        # 删除除了带有 _merge 后缀的文件之外的其他 CSV 文件
        for file in Path(folder_path).glob("*.csv"):
            if "_merge" not in file.name:
                file.unlink()

        return merge_files_list
    else:
        return []


# 解压并删除zip文件
def unzip_and_delete_zip(zip_files, folder_path):
    # 查找指定币种的zip文件
    for zip_file in zip_files:
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(folder_path)  # 解压
        # 删除原zip文件
        # os.remove(zip_file)
    # print(f"{coin_name}解压完成")


def process_single_file(file):
    # 读取文件的第一行
    with open(file, 'r') as f:
        first_line = f.readline().strip()  # .strip() 移除可能的前后空白字符

    # 检查第一行是否包含任何预期的列名
    has_header = any(col_name in first_line for col_name in ["open_time", "open", "high", "low", "close"])

    # 根据文件是否有列名来读取数据
    df = pd.read_csv(file, header=0 if has_header else None)

    # 币安API返回的部分文件没有列名，如果没有列名，需要手动指定
    if not has_header:
        df.columns = [
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "count",
            "taker_buy_volume", "taker_buy_quote_volume", "ignore"
        ]
    # 将列名映射到新的列名
    column_mapping = {
        "open_time": "candle_begin_time",
        "count": "trade_num",
        "taker_buy_volume": "taker_buy_base_asset_volume",
        "taker_buy_quote_volume": "taker_buy_quote_asset_volume"
    }
    df.rename(columns=column_mapping, inplace=True)

    # 添加新的列
    df['symbol'] = file.split(os.sep)[-1].split('USDT')[0] + '-USDT'
    # 注意：avg_price_1m 和 avg_price_5m 需要后续计算

    # 转换时间格式
    df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'], unit='ms')

    # 删除不需要的列
    df.drop(['close_time', 'ignore'], axis=1, inplace=True)

    return df


def process_coin_files(files):
    dataframes = Parallel(n_jobs=max(os.cpu_count() - 1, 1))(
        delayed(process_single_file)(file) for file in files
    )
    merged_df = pd.concat(dataframes)

    merged_df.sort_values(by='candle_begin_time', inplace=True)
    merged_df.drop_duplicates(subset=['candle_begin_time'], inplace=True, keep='last')
    merged_df.reset_index(drop=True, inplace=True)
    # 填充空缺的数据
    start_date = merged_df.iloc[0]['candle_begin_time']
    end_date = merged_df.iloc[-1]['candle_begin_time']
    benchmark = pd.DataFrame(pd.date_range(start=start_date, end=end_date, freq='1T'))  # 创建开始至回测结束时间的1H列表
    benchmark.rename(columns={0: 'candle_begin_time'}, inplace=True)
    merged_df = pd.merge(left=benchmark, right=merged_df, on='candle_begin_time', how='left', sort=True, indicator=True)
    merged_df['close'] = merged_df['close'].fillna(method='ffill')
    merged_df['symbol'] = merged_df['symbol'].fillna(method='ffill')
    for column in ['open', 'high', 'low']:
        merged_df[column] = merged_df[column].fillna(merged_df['close'])
    _ = ['volume', 'quote_volume', 'trade_num', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']
    merged_df[_] = merged_df[_].fillna(0)
    # merged_df = merged_df[merged_df['_merge'] == 'left_only']

    # 将索引转换为DatetimeIndex，如果它还不是
    merged_df.set_index('candle_begin_time', inplace=True)

    merged_df['avg_price_1m'] = merged_df['quote_volume'] / merged_df['volume']
    merged_df['avg_price_5m'] = merged_df['quote_volume'].rolling(window=5).sum() / merged_df['volume'].rolling(
        window=5).sum()
    merged_df['avg_price_5m'] = merged_df['avg_price_5m'].shift(-4)
    merged_df['avg_price_1m'].fillna(merged_df['open'], inplace=True)
    merged_df['avg_price_5m'].fillna(merged_df['open'], inplace=True)

    hourly_df = merged_df.resample('1H').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'quote_volume': 'sum',
        'trade_num': 'sum',
        'taker_buy_base_asset_volume': 'sum',
        'taker_buy_quote_asset_volume': 'sum',
        'symbol': 'first',
        'avg_price_1m': 'first',
        'avg_price_5m': 'first',
    })
    hourly_df.reset_index(inplace=True)

    return hourly_df


def find_trigger_index(goals, prices_list, trigger_type):
    """
    :param goals: 目标价格列表
    :param prices_list: 价格列表
    :param trigger_type: 触发类型，'loss'表示止损，'profit'表示止盈
    :return: 触发索引列表
    """
    trigger_indexes = []
    for goal, prices in zip(goals, prices_list):
        if trigger_type == 'loss':
            trigger_index = next((i for i, price in enumerate(prices) if goal > price), None)
        elif trigger_type == 'profit':
            trigger_index = next((i for i, price in enumerate(prices) if goal < price), None)
        else:
            raise ValueError('止盈止损类型非法')
        trigger_indexes.append(trigger_index if trigger_index is not None else float('inf'))
    return trigger_indexes


def calculate_stop(row):
    """
    计算止盈止损触发情况
    """
    if row['stop_loss_trigger'] == float('inf') and row['stop_profit_trigger'] == float('inf'):
        return 0  # 止盈止损都没触发
    elif row['stop_loss_trigger'] < row['stop_profit_trigger']:
        return -1  # 止损先触发
    elif row['stop_loss_trigger'] > row['stop_profit_trigger']:
        return 1  # 止盈先触发
    else:
        return 2  # 在同一小时同时触发止盈和止损


def process_single_combination(df, stop_profit, stop_loss):
    _ = df.copy()
    # print(f'stop_profit: {stop_profit}, stop_loss: {stop_loss}')
    _['stop_profit_price'] = _['avg_price_1m'] * (1 + stop_profit)
    _['stop_loss_price'] = _['avg_price_1m'] * (1 + stop_loss)
    # 创建最高价和最低价的辅助列，存储未来24小时的价格数组
    low_list = []
    high_list = []
    for index, row in _.iterrows():
        high_prices = _.loc[index:index + 23, 'high'].tolist()
        low_prices = _.loc[index:index + 23, 'low'].tolist()
        extra_prices = _.loc[index + 24:index + 24, 'avg_price_1m'].tolist()
        high_list.append(high_prices + extra_prices)
        low_list.append(low_prices + extra_prices)
    _['high_list'] = high_list
    _['low_list'] = low_list
    _['stop_profit_trigger'] = find_trigger_index(_['stop_profit_price'], _['high_list'], 'profit')
    _['stop_loss_trigger'] = find_trigger_index(_['stop_loss_price'], _['low_list'], 'loss')

    # 计算最终的stop值
    _[f'stop[{stop_profit}_{stop_loss}]'] = _.apply(calculate_stop, axis=1)

    return _[f'stop[{stop_profit}_{stop_loss}]']


def process_stop(df, stop_loss_list, stop_profit_list, n_jobs=-1):
    if not calc_stop:
        return df
    # print(f'开始计算{coin_name}止盈止损状态数据')
    stop_all_list = list(product(stop_profit_list, stop_loss_list))
    results_dfs = Parallel(n_jobs=n_jobs)(
        delayed(process_single_combination)(df, stop_profit, stop_loss) for stop_profit, stop_loss in stop_all_list)
    results_combined = pd.concat(results_dfs, axis=1)
    # 合并结果
    df_final = pd.concat([df, results_combined], axis=1)

    return df_final


def get_merge_csv_files(folder_path):
    csv_files = glob(os.path.join(folder_path, '*.csv'))

    grouped_files = {}
    for file in csv_files:

        # if '_merge' in file:
        if '_merged' in os.path.basename(file):
            continue
        coin_name = os.path.basename(file).split('-')[0]

        grouped_files.setdefault(coin_name, []).append(file)

    for coin_name, files in grouped_files.items():
        try:
            hourly_df = process_coin_files(files)

            df_final = process_stop(hourly_df, stop_loss_list, stop_profit_list)

            df_final.to_csv(os.path.join(folder_path, f'{coin_name}_merged.csv'), index=False)

        except Exception as exc:
            print(f"\n {coin_name}生成过程中出错: {exc}")


# 删除未合并的CSV文件
def delete_unmerged_csv_files(folder_path):
    # 查找文件夹中所有的CSV文件
    csv_files = glob(os.path.join(folder_path, '*.csv'))

    for csv_file in csv_files:
        # 检查文件名是否不包含 '_merged'
        if '_merged' not in os.path.basename(csv_file):
            os.remove(csv_file)  # 删除文件


if __name__ == "__main__":
    target = 'spot'
    # 检查是否有足够的命令行参数
    if len(sys.argv) > 1:
        target = sys.argv[1]

    coins_to_clean = extract_coin_names(下载文件夹)
    merge_csvs = all_merge_csv(下载文件夹)

    # 如果任务中断，识别断点，继续清理
    if len(merge_csvs) >= 2:
        index_next_clean = coins_to_clean.index(merge_csvs[-2])
        coins_to_clean = coins_to_clean[index_next_clean:]

    mode = "现货数据" if target == "spot" else "合约数据"
    with tqdm(total=len(coins_to_clean), desc="总体进度", unit="step") as pbar:
        for coin_name in coins_to_clean:
            # 步骤1: 解压
            zip_files = glob(os.path.join(下载文件夹, f'{coin_name}*.zip'))
            file_num = len(zip_files)
            pbar.set_description(f"📦 正在解压{file_num}个{coin_name}的zip文件")
            unzip_and_delete_zip(zip_files, 下载文件夹)  # 解压指定币种的zip文件并删除

            # 步骤2: 清洗合并
            pbar.set_description(f"🔄 正在清洗合并{coin_name}的{file_num}个K线数据csv文件")
            get_merge_csv_files(下载文件夹)

            # 步骤3: 删除这个币种的一分钟CSV,完成处理
            delete_unmerged_csv_files(下载文件夹)
            pbar.update(1)
            pbar.set_description(f"💯 {file_num}个{coin_name}{mode}️清洗完成，已合并保存")
            print('')
            time.sleep(1)
    pbar.close()
    print("\n所有币种清洗完成")

