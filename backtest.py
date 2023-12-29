from instruction import Instruction
from dataLoader import MarketData, AggregationTrade, Kline, Orderbook, orderbookLoader
from account import Account

import orjson
from abc import abstractmethod
from exchange.utils.strconv import base36_encode
from collections import deque
from typing import List, Optional
import os
import pandas as pd
import numpy as np
import datetime
import time

class Backtest:
    def __init__(
        self,
        file_depth: str,
        file_trade: str,
        file_write: str,
        ob_level: int,
        start_time: datetime,
        end_time: datetime,
        symbols: list[str],  # list of symbols
        strategy_name: str,
        market_data_type: str,
        market_type: str,
    ) -> None:
        
        self._first_tick_ts = start_time.timestamp()
        self._last_tick_ts = end_time.timestamp()
        self.start_time = time.time()
        self.account = Optional[Account] = None
        self.strategy_name = strategy_name

        self.instruction_pending = Optional[list[Instruction]] = None
        self.instruction_completed = Optional[list[Instruction]] = None
        self.instruction_cancelled = Optional[list[Instruction]] = None
        self.symbols = symbols
        self.position = dict.fromkeys(symbols)  # right now only btcusdt
        self.market_type = market_type
        self.market_data = Optional[MarketData] = None
        self.market_data_type = market_data_type
        self.ob_level = Optional[int] = None

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
        t = base36_encode(int(time.time() * 1000))  # convert to base36 to save space
        return f"{self.strategy_id}_{t}"
    
    async def create_account(self) -> Account:
        account_id = self.generate_account_id()
        test_account = Account(
            account_id=account_id,
        )
        return test_account

    async def create_dataLoader(self):
        if self.market_data_type == 'orderbook':
            self.dataLoader = orderbookLoader(
            file_depth=self.file_depth,
            file_trade=self.file_trade,
            file_write=self.file_write,
            ob_level=self.ob_level,
            first_tick_ts=self._first_tick_ts,
            last_tick_ts=self._last_tick_ts,
        )
                                                        # TODO kines and aggragationtrade data loader
                                                        # TODO kines and aggragationtrade data parser
    async def _parse_orderbook_data(self, line: list, market_type: str) -> Orderbook:
        Bids = deque()
        Asks = deque()
        b_cols = range(2, 2 * self.ob_level + 2, 2)
        a_cols = range(2 * self.ob_level + 2, 4 * self.ob_level + 2, 2)
        for i in b_cols:
            Bids.append((line[i, i+1]))
        for i in a_cols:
            Asks.append((line[i, i+1]))
        orderbook = Orderbook(
            symbol = line[0],    # TODO bad code!!
            ob_level = self.ob_level,
            market_type = market_type,
            bids = Bids,
            asks = Asks,
            ts = line[1]
        )

        return orderbook

    
    async def start(self):
        await self.logger.init()   # TODO 生成logger
        self.account = await self.create_account()
        await self.start_test()

    async def start_test(self):

        def _convert_or_nan(x, func):
            if x == np.nan:
                return np.nan
            try:
                return func(x)
            except:
                raise Exception(f"Failed to convert {x} to {func}")

        async for depth_data in self.dataLoader.tick_depth_data_feed():
            if depth_data == None:   # TODO not in the target backtest time interval
                continue
            else:
                self.market_data.order_book.append(self._parse_orderbook_data(self, depth_data, self.market_type))   # TODO 这个market type代表什么，在backtest init中
                await self.before_tick()
                await self.tick()
                await self.post_tick()

                if await self.is_signal_ready():
                    await self.signal_processing()
                    await self.post_signal()








    




