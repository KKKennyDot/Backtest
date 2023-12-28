import json
import os
import pandas as pd
import numpy as np
import datetime
import time
import bisect


'''global parameter'''
timestamp_idx = 1               # timestamp index in depth data
timestamp_trade_idx = 4         # timestamp index in trade data
start_timestamp_trade_idx = 7   # new added columns in trade data
end_timestamp_trade_idx = 8
ob_level = 20
cols_trade = ['id', 'price', 'qty', 'quoteQty', 'timestamp', 'isBuyerMaker', 'isBestMatch']

file_0 = './data/BTCUSDT-trades-2023-12-20.csv'
file_1 = './data/BTCUSDT-trades-2023-12-21.csv'
file_depth = './data/btc_depth_local'
file_write = 'btc_output_local.csv'
folder_write_1 = './data/1'
folder_write_2 = './data/2'


def cal_runtime(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()

        runtime= end_time - start_time
        print(f"{func.__name__} runtime: {runtime: .3f}s")
        return result
    return wrapper

def process_line(line):
    line = json.loads(line)
    res = [line['symbol'], line['ts']]
    for side  in ['bids', 'asks']:
        desending_order = True if side=='bids' else False
        sorted_depth = sorted(line[side], key=lambda x: x['price'], reverse=desending_order)
        for d in sorted_depth:
            res.append(d['price'])
            res.append(d['qty'])
    return res

@cal_runtime
def read_depth_data(filename, folder_start_position, folder_write, ob_level):
    position_file = os.path.join(folder_start_position, 'position_file.txt')
    if not os.path.exists(position_file):
        start_position = 0
    else: 
        with open(position_file, 'r') as f_pos:    # 读取新增数据的开始位置（上一次读取的末尾位置）该文件在上一次的文件夹中读取
            start_position = int(f_pos.read())
    print(start_position)
    df = []
    with open(filename, 'r') as f:
        f.seek(start_position)
        cols = ['symbol', 'timestamp']
        cols += [f"b{i}{suffix}" for i in range(0, ob_level) for suffix in ["", "_vol"]]
        cols += [f"a{i}{suffix}" for i in range(0, ob_level) for suffix in ["", "_vol"]]
        for line in f:
            df.append(process_line(line))
        end_position = f.tell()
    with open(os.path.join(folder_write, 'position_file.txt'), 'w') as f_pos_end:
            f_pos_end.write(str(end_position))
            
    df = pd.DataFrame(df, columns=cols)
    df['timestamp'] = (df['timestamp'] / 1e6).astype('int64')  # align with the trade data format
    df.sort_values(by='timestamp', inplace=True)
    return df, cols

@cal_runtime
def read_trade_data(filename, timestamp_depth):
    df_trade = []
    for file in filename:
        tmp = pd.read_csv(file, header=None)
        tmp.rename(columns=dict(zip(range(9), cols_trade)), inplace=True)
        tmp = tmp[(tmp['timestamp'] >= timestamp_depth.min()) & (tmp['timestamp'] <= timestamp_depth.max())]
        df_trade.append(tmp)

    df_trade = pd.concat(df_trade, axis = 0)
    df_trade.reset_index(inplace=True, drop = True)
    df_trade.sort_values(by='timestamp', inplace = True)
    return df_trade


@cal_runtime
def is_duplication_depthwise(df, cols, ob_level):   # an snapshot of orderbook should has different price at different depth
    b_cols = range(2, 2 * ob_level + 2)
    a_cols = range(2 * ob_level + 2, 4 * ob_level + 2)
    df_arr = df.to_numpy()
    df_arr_new = []
    for row in df_arr:
        row = np.append(row, np.any(np.diff(row[b_cols]) == 0))  # maybe diff too large?
        row = np.append(row, np.any(np.diff(row[a_cols]) == 0))
        df_arr_new.append(row)
    cols += ['b_dup_count', 'a_dup_count']
    df = pd.DataFrame(df_arr_new, columns=cols)
    return df[df['a_dup_count'] == True]['symbol'].count()

@cal_runtime
def indexing(df_trade, df_depth, folder_write, file_write):
    '''transform to Numpy array for acceleration'''
    i=0
    data_list = []
    df_trade_arr = df_trade.to_numpy()
    df_arr = df_depth.to_numpy()
    for idx in range(len(df_trade_arr)):
        data_trade = list(df_trade_arr[idx])
        if(data_trade[timestamp_trade_idx] <= df_arr[i][timestamp_idx]):
            data_trade = np.append(data_trade, [df_arr[i-1][timestamp_idx], df_arr[i][timestamp_idx]])   # next trade data in the same interval as last one 
            data_list.append(data_trade)
            continue
        while(data_trade[timestamp_trade_idx] > df_arr[i][timestamp_idx]):   # trade data later than current timestamp: next depth data
            i+=1
        data_trade = np.append(data_trade, [df_arr[i-1][timestamp_idx], df_arr[i][timestamp_idx]])   # find the right interval
        data_list.append(data_trade)
        
    df_trade = pd.DataFrame(data_list)
    df_trade.rename(columns=dict(zip(range(9), cols_trade + ['start_timestamp', 'end_timestamp'])), inplace=True)
    df_trade[['isBuyerMaker', 'isBestMatch']] = df_trade[['isBuyerMaker', 'isBestMatch']].astype(bool)
    df_trade[['id', 'timestamp', 'start_timestamp', 'end_timestamp']] = df_trade[['id', 'timestamp', 'start_timestamp', 'end_timestamp']].astype('int64')
    df_trade.to_csv(os.path.join(folder_write, file_write), index = False, header = True)
    return df_trade

def data_proc(file_trade: list, file_depth: list, file_write: str, folder_start_write: str, folder_end_write: str, ob_level: int):
    df_depth, cols_depth = read_depth_data(file_depth, folder_start_write, folder_end_write,  ob_level)
    df_trade = read_trade_data(file_trade, df_depth.timestamp)

    if is_duplication_depthwise(df_depth, cols_depth, ob_level) == False:
        df_trade = indexing(df_trade, df_depth, folder_end_write, file_write)
    else: print('Orderbook depth data lacks integrity!')

def read_test(filename, ob_level):
    with open(filename, 'r+') as f:
       content = f.read()
       f.write(content)

if __name__ == '__main__':
    file_trade = [file_0, file_1]
    file_depth = file_depth
    data_proc(file_trade, file_depth, file_write, folder_write_1, folder_write_1, ob_level)
    read_test(file_depth, ob_level)
    data_proc(file_trade, file_depth, file_write, folder_write_1, folder_write_2, ob_level)
    

