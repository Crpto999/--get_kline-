import os
import subprocess
from config import *

'''
接口来源：币安
官方接口github：https://github.com/binance/binance-public-data
数据来源：https://data.binance.vision
'''
下载的类型 = ['spot']  # 'swap',#'spot'

scripts = [
    ('1_get_binance_data_zip.py', True),  # 需要模式参数
    ('2_release_zip_and_clean_data.py', True),  # 不需要模式参数
    ('3_merge_to_orginal_csv.py', True)  # 需要模式参数
]
os.makedirs(下载文件夹, exist_ok=True)

for mode in 下载的类型:
    if mode == 'spot':
        os.makedirs(现货K线存放路径, exist_ok=True)
    elif mode == 'swap':
        os.makedirs(永续合约K线存放路径, exist_ok=True)
    for script in scripts:
        # if mode == 'swap' and script[0] == '1_get_binance_data_zip.py':
        #     print(f'跳过 {script[0]}')
        #     continue
        # 检查脚本是否需要额外的模式参数
        if script[1]:  # 如果需要额外参数
            subprocess.run(['python', script[0], mode])
        else:
            subprocess.run(['python', script[0]])
        print(f'{script[0]} 运行完成')

    print(f'{mode} 数据全部下载完成')
