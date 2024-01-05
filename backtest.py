from instruction import Instruction
from dataLoader import MarketData, AggregationTrade, Kline, Orderbook, orderbookLoader
from account import Account

import orjson
from abc import abstractmethod
import base36
from collections import deque
from typing import List, Optional
import os
import pandas as pd
import numpy as np
from datetime import datetime
import time

class Backtest:
    def __init__(
        self,
        file_depth: str,
        file_trade: str,
        file_write: str,
        ob_level: int,
        start_time: str,  # _
        end_time: str,
        symbols: list[str],  # TODO list of symbols
        strategy_name: str,
        market_data_type: str,
        market_type: str,
    ) -> None:
        self.file_depth = file_depth
        self.file_trade = file_trade
        self.file_write = file_write
        self._first_tick_ts = int(datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S.%f").timestamp() * 1e9)  # TODO 统一到19位精度
        self._final_tick_ts = int(datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S.%f").timestamp() * 1e9)
        self._last_tick_ts: Optional[int] = None
        self.account: Optional[Account] = None
        self.strategy_name = strategy_name
        self.symbols = symbols
        self.market_type = market_type
        self.market_data: Optional[MarketData] = MarketData()
        self.trade_list: Optional[list]()
        self.market_data_type = market_data_type
        self.ob_level = ob_level

        self.dataLoader: Optional[orderbookLoader] = None # TODO 今后也许可能有kline loader等，如何实现泛用

        self.instruction_pending: Optional[deque[Instruction]]()
        self.instruction_completed: Optional[deque[Instruction]]()
        self.instruction_cancelled: Optional[deque[Instruction]]()
        self.priority_qty_per_Buyprice: Optional[dict[float, list]]() 
        self.priority_qty_per_Sellprice: Optional[dict[float, list]]() 
        # TODO 按照时间遍历instruction，每次将该instruction的price剩余的qty累加，得到在该instruction前
        # 的所有instruction剩余的优先级更高的挂单qty的总和，加上在当前ts的depth数据可观察到的原有挂单qty构建队列
        self.trade_bid_dict: Optional[dict[float, list[(float, str)]]]()
        self.trade_ask_dict: Optional[dict[float, list[(float, str)]]]()

    async def find_instruction_from_uid(self, uid: str):
        for index, inst in enumerate(self.Instruction_pending):
            if inst.uid == uid:
                return index

    @abstractmethod
    async def is_signal_ready(self):
        pass
    @abstractmethod
    async def signal_processing(self):
        pass
    @abstractmethod
    async def post_signal(self):
        pass
    @abstractmethod
    async def tick(self):
        pass
    @abstractmethod
    async def before_tick(self):
        pass
    @abstractmethod
    async def post_tick(self):
        pass

    async def generate_account_id(self) -> str:
        t = base36.dumps(int(time.time() * 1000))  # convert to base36 to save space
        return f"{self.strategy_name}_{t}"
    
    async def create_account(self):
        account_id = await self.generate_account_id()
        self.account = Account(
            account_id=account_id,
        )

    async def create_dataLoader(self):
        if self.market_data_type == 'orderbook':
            self.dataLoader = orderbookLoader(
            file_depth=self.file_depth,
            file_trade=self.file_trade,
            file_write=self.file_write,
            ob_level=self.ob_level,
            first_tick_ts=self._first_tick_ts,
            last_tick_ts=self._final_tick_ts,
        )
                                                        # TODO kines and aggragationtrade data loader
                                                        # TODO kines and aggragationtrade data parser
    async def parse_orderbook_data(self, line: list, market_type: str) -> Orderbook:
        Bids = deque()
        Asks = deque()
        b_cols = range(2, 2 * self.ob_level + 2, 2)
        a_cols = range(2 * self.ob_level + 2, 4 * self.ob_level + 2, 2)
        for i in b_cols:
            Bids.append((line[i], line[i+1]))
        for i in a_cols:
            Asks.append((line[i], line[i+1]))  # TODO 以（price，vol）类型
        orderbook = Orderbook(
            symbol = line[0],    # TODO bad code!!
            ob_level = self.ob_level,
            market_type = market_type,
            bids = Bids,
            asks = Asks,
            ts = line[1]
        )

        return orderbook

    async def before_tick(self):
        if self._last_tick_ts != -1:
            trade_list = []
            async for trade_data in self.dataLoader.tick_trade_data_feed():
                if trade_data == None: continue
                start_index = self.dataLoader.trade_cols['start_timestamp']
                end_index = self.dataLoader.trade_cols['end_timestamp']
                price_index = self.dataLoader.trade_cols['price']
                qty_index = self.dataLoader.trade_cols['qty']
                if trade_data[end_index] < self.market_data.order_book.ts and trade_data[start_index] == self._last_tick_ts:
                    trade_list.append((trade_data[price_index], trade_data[qty_index]))
            trade_list_sorted = sorted(trade_list, key=lambda x:x[0])  # price ascending
            self.trade_list = trade_list_sorted  # TODO 如何结合depth data  以及是否要舍弃排序

        self.trade_bid_dict = dict(zip(self.market_data.order_book.get_price_list('bid'), [[(x, 'depth')] for x in self.market_data.order_book.get_vol_list('bid')]))
        self.trade_ask_dict = dict(zip(self.market_data.order_book.get_price_list('ask'), [[(x, 'depth')] for x in self.market_data.order_book.get_vol_list('ask')]))  # TODO 没有inst存在的价位用depth来替代
    
    async def post_tick(self):
        self.priority_qty_per_Buyprice: Optional[dict[float, list]]()   # TODO 清除上一个tick的priority dict
        self.priority_qty_per_Sellprice: Optional[dict[float, list]]() 
        self.account.update_metric()

    async def start_test(self):

        def _convert_or_nan(x, func):
            if x == np.nan:
                return np.nan
            try:
                return func(x)
            except:
                raise Exception(f"Failed to convert {x} to {func}")
        index = 0
        async for depth_data in self.dataLoader.tick_depth_data_feed():
            if depth_data == None:   # TODO not in the target backtest time interval
                continue
            else:
                if index == 0:
                    self._last_tick_ts == -1  # TODO 第一条数据故而没有上一个tick用-1替代
                else: 
                    self._last_tick_ts = self.market_data.order_book.ts

                self.market_data.order_book = await self.parse_orderbook_data(depth_data, self.market_type)   # TODO 这个market type代表什么，在backtest init中                
                
                await self.before_tick()
                await self.tick()
                await self.post_tick()

                if await self.is_signal_ready():
                    await self.signal_processing()
                    await self.post_signal()
                index += 1
                    
    async def start(self):
        # await self.logger.init()   # TODO 生成logger
        await self.create_account()
        await self.create_dataLoader()
        await self.start_test()







    




