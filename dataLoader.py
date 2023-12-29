import time
import json
import os
from collections import deque
from typing import List, Optional
import aiofiles
import asyncio

class AggregationTrade:
    event_time: int
    recv_time: int
    symbol: str
    price: float
    quantity: float
    trade_time: int

    def __init__(
        self,
        event_time: int,
        symbol: str,
        price: float,
        quantity: float,
        trade_time: int,
    ):
        self.event_time = event_time
        self.symbol = symbol
        self.price = price
        self.quantity = quantity
        self.trade_time = trade_time
        self.recv_time = time.time_ns()


class Kline:
    def __init__(
        self,
        market_type: str,
        event_time: int,
        start_time: int,
        close_time: int,
        symbol: str,
        finished: bool,
        # deprecated, using volume instead
        quantity: float,
        open_price: float,
        close_price: float,
        high_price: float,
        low_price: float,
        interval: str,
        num_of_trades: int,
        quote_asset_volume: float,
        volume: float,
    ):
        self.market_type = market_type
        self.event_time = event_time
        self.start_time = start_time
        self.close_time = close_time
        self.symbol = symbol
        self.finished = finished
        self.open_price = open_price
        self.close_price = close_price
        self.high_price = high_price
        self.low_price = low_price
        self.quantity = quantity
        self.interval = interval
        self.recv_time = time.time_ns()
        self.num_of_trades = num_of_trades
        self.quote_asset_volume = quote_asset_volume
        self.volume = volume

    def to_json(self):
        return orjson.dumps(self, default=lambda o: o.__dict__)


class Orderbook:
    def __init__(self, symbol: str, ob_level: int, market_type: str):
        self.symbol = symbol
        self.ob_level = ob_level
        self.market_type = market_type
        self.bids = deque()
        self.asks = deque()
        self.ts = 0


class MarketData:
    def __init__(self):
        self.aggregation_trade: Optional[AggregationTrade] = None
        self.kline: Optional[Kline] = None
        self.order_book: Optional[Orderbook] = None



class orderbookLoader:

    def __init__(
        self,
        file_depth: str,
        file_trade: str,
        file_write: str,
        ob_level: int,
        first_tick_ts: int,
        last_tick_ts: int,
    ) -> None:
        self.file_depth = file_depth
        self.file_trade = file_trade
        self.file_write = file_write
        self._first_tick_ts = first_tick_ts
        self._last_tick_ts = last_tick_ts
        self.depthNum = 0
        self.tradeNum = 0

        self.current_time = time.time()
        self.ob_level = ob_level

        depth_cols = ['symbol', 'timestamp']
        depth_cols += [f"b{i}{suffix}" for i in range(0, ob_level) for suffix in ["", "_vol"]]
        depth_cols += [f"a{i}{suffix}" for i in range(0, ob_level) for suffix in ["", "_vol"]]
        self.depth_cols = dict(zip(depth_cols, range(2 + ob_level * 4)))
        trade_cols = ['id', 'price', 'qty', 'quoteQty', 'timestamp', 'isBuyerMaker', 'isBestMatch', 'start_timestamp', 'end_timestamp']
        self.trade_cols = dict(zip(trade_cols, range(9)))
        
    
    async def process_depth_line(self, line) -> Optional[list]:
        line = json.loads(line)
        if line['ts'] < self._first_tick_ts or line['ts'] > self._last_tick_ts:
            return None
        res = [line['symbol'], int(line['ts'] / 1e6)]
        for side  in ['bids', 'asks']:
            desending_order = True if side=='bids' else False
            sorted_depth = sorted(line[side], key=lambda x: x['price'], reverse=desending_order)
            for d in sorted_depth:
                res.append(d['price'])
                res.append(d['qty'])
        return res
    
    async def process_trade_line(self, line) -> Optional[list]:
        line = line.split(',')
        if line[self.trade_cols['timestamp']] < self._first_tick_ts or line[self.trade_cols['timestamp']] > self._last_tick_ts:
            return None
        res = []
        res.append(line[self.trade_cols['id']])
        res.append(float(line[self.trade_cols['price']]))
        res.append(float(line[self.trade_cols['qty']]))
        res.append(float(line[self.trade_cols['quoteQty']]))
        res.append(int(line[self.trade_cols['timestamp']]))
        res.append(bool(line[self.trade_cols['isBuyerMaker']]))
        res.append(bool(line[self.trade_cols['isBestMatch']]))
        res.append(int(line[self.trade_cols['start_timestamp']]))
        res.append(int(line[self.trade_cols['end_timestamp']]))
        return res

    async def tick_depth_data_feed(self) -> Optional[list]:  # TODO 是否是return None
        self.current_time = time.time()
        if not os.path.exists(self.file_depth):
            raise Exception('There is no such orderbook depth data file')
        
        async with aiofiles.open(self.file_depth, 'r') as f:
            # first_line = await f.readline()    # no header for both trade and depth data          
            while True:
                line = await f.readline()
                if not line:
                    break
                tick_depth_data = await self.process_depth_line(line) 
                if tick_depth_data != None: self.depthNum += 1  # TODO 能否正常判断
                yield  tick_depth_data # one row at a time
            if self.depthNum != 0:
                raise Exception('There is no orderbook depth data during the appointed backtest time period')

    async def tick_trade_data_feed(self) -> Optional[list]:  # TODO 是否是return None
        self.current_time = time.time()
        if not os.path.exists(self.file_trade):
            raise Exception('There is no such orderbook trade data file')

        async with aiofiles.open(self.file_trade, 'r') as f:
            # first_line = await f.readline()    # no header for both trade and depth data
            while True:
                line = await f.readline()
                if not line:
                    break
                tick_trade_data = await self.process_trade_line(line) 
                if tick_trade_data != None: self.tradeNum += 1
                yield  tick_trade_data # one row at a time
            if self.tradeNum == 0:
                raise Exception('There is no orderbook trade data during the appointed backtest time period')
