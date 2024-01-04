import asyncio
import numpy as np
from backtest import Backtest
from dataLoader import (
    Orderbook,
    MarketData,
    orderbookLoader
)

import pandas as pd
import base36
from dataclasses import dataclass
from datetime import datetime
from pytimeparse.timeparse import timeparse
import argparse
import yaml
import dacite

class MACD(Backtest):
    def __init__(
        self,
        file_depth: str,
        file_trade: str,
        file_write: str,
        ob_level: int,
        start_time: str,  # "%Y-%m-%d %H:%M:%S"
        end_time: str,
        symbols: list[str],  # TODO list of symbols
        strategy_name: str,
        market_data_type: str,
        market_type: str,
        n_tick: int,
        time_limit: int,
    ):
        super().__init__(
            file_depth,
            file_trade,
            file_write,
            ob_level,
            start_time,  # "%Y-%m-%d %H:%M:%S"
            end_time,
            symbols,  # TODO list of symbols
            strategy_name,
            market_data_type,
            market_type,
        )
        self.n_tick = n_tick
        self.time_limit = time_limit
        self.p = {
            "max_position": 0.95,
            "sizing_coeff": 1.0 # TODO 这里sizing coeff是什么意思
        }

    async def init_strategy(self):
        self.symbols = ['btcusdt']  # TODO 目前只有一个symbol

    def sizing_function(self, raw_size: float):
        """init sizing function"""
        target_size = np.clip(self.p['sizing_coeff'] * raw_size, -1, 1)
        # self.logger.debug(f"Raw size: {raw_size}; Scaled size: {target_size}")
        return target_size
    
    async def cal_available_vol(self, price):   # 需要trade数据做支撑
        available_vol = 0
        for trade_data in self.trade_list:
            if trade_data[0] <= price:
                available_vol += trade_data[1]
        return available_vol    
    
    async def transaction(self, ts):  # TODO 如何结合trade数据和depth数据得出最大的volume
        for inst in self.instruction_pending: 
            await inst.update_inst(ts)
            if inst.isExcceedTime():  # if time excceed the limit
                self.instruction_cancelled.append(inst)
                continue
            excute_price = inst.get_limit_price()
            available_vol = await self.cal_available_vol(excute_price)
            # TODO 检查depth数据，看是否能够吃掉
            if available_vol >= inst.get_quantity_remain():
                excute_qty = inst.get_quantity_remain()  # TODO 如何定义qty
                await inst.excute_instruction(self, excute_qty, excute_price)
                await self.account.update_position(
                    symbol = inst.symbol, 
                    traded_qty = excute_qty, 
                    target_qty = inst.get_target_quantity(), 
                    price = excute_price, 
                    ts = ts, 
                    tag = inst.get_tag()
                )
            else:
                excute_qty =  available_vol # TODO 如何定义qty
                await inst.excute_instruction(self, excute_qty, excute_price)
                await self.account.update_position(
                    symbol = inst.symbol, 
                    traded_qty = excute_qty,   # TODO 此处有正负
                    target_qty = inst.get_target_quantity(), 
                    price = excute_price, 
                    ts = ts, 
                    tag = inst.get_tag()
                )
                
            if inst.isCompleted():
                self.instruction_completed.append(inst)
    
        for inst in self.instruction_pending:
            if inst.isExcceedTime():
                self.instruction_pending.remove(inst)
            if inst.isCompleted():
                self.instruction_pending.remove(inst)

    async def tick(self):
        cur_ts = self.market_data.order_book.ts
        cur_symbol = self.market_data.order_book.symbol
        cur_price = self.market_data.order_book.bids[0][0]   # TODO 用哪个价格来作为计算持仓价值的价格

        await self.account.update_position_value(cur_symbol, cur_price)

        await self.transaction(cur_ts)   # TODO 顺序很重要，新发生的instruction下一tick才进行交易

        # signal and trading logic
        macd = 10   # TODO 暂时是乱定义的，需要修改
        macd_prev = 9
        self.n_tick += 1  # TODO single symbol在每次tick后都增加1，但是多symbol处理情况不同
        # Signals
        if macd > 0 and macd_prev < 0:
            self.account.directions[cur_symbol] = 1
            self.account.macd_diff[cur_symbol] = macd-macd_prev
        elif macd < 0 and macd_prev > 0:
            self.account.directions[cur_symbol] = -1
            self.account.macd_diff[cur_symbol] = macd_prev-macd
        else:
            self.account.directions[cur_symbol] = 0
            self.account.macd_diff[cur_symbol] = 0
        
        return None

    async def post_signal(self):
        self.account.directions = dict.fromkeys(self.symbols, None)
        self.n_tick = 0

    async def is_signal_ready(self):
        if len(self.symbols) == self.n_tick:
            return True
        return False
    
    async def signal_processing(self):
        qty = self.sizing_function(0.05)  # TODO 如何确定qty(0.5 for test right now)
        cur_ts = self.market_data.order_book.ts
        for symbol, direction in self.directions.items():
            if direction == 0:
                continue
            if direction == 1:
                # self.logger.info(f"Buy {symbol}")
                inst = await self.account.create_instruction(
                    symbol=symbol, 
                    market_type = self.market_data.order_book.market_type, 
                    limit_price=self.market_data.order_book.bids[0], 
                    quantity=qty, 
                    direction=direction, 
                    time_limit = self.time_limit,
                    tag = 'empty',  # 如何判断是开仓还是平仓,除了买卖信号外，应该还有一种信号是开仓平仓
                    ts = cur_ts
                ) # TODO 改正函数格式
                self.instruction_pending.append(inst)
            elif direction == -1:
                # self.logger.info(f"Sell {symbol}")
                inst = await self.account.create_instruction(
                    symbol = symbol, 
                    market_type = self.market_data.order_book.market_type, 
                    limit_price = self.market_data.order_book.bids[0], 
                    quantity = qty, 
                    direction = direction, 
                    time_limit = self.time_limit,
                    tag = 'empty',   
                    ts = cur_ts
                ) # TODO 改正函数格式
                self.instruction_pending.append(inst)
                
    async def start_strategy(self):
        await self.init_strategy()
        await self.start()

@dataclass
class StrategyConfig:
    file_depth: str
    file_trade: str
    file_write: str
    ob_level: int
    start_time: str
    end_time: str
    symbols: list[str] # TODO list of symbols
    strategy_name: str
    market_data_type: str
    market_type: str
    n_tick: int
    time_limit: int

        

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--f", type=str, default="conf/conf_sample.yaml"
    )
    args = parser.parse_args()

    try:
        with open(args.f, "r") as f:
            conf = yaml.load(f, Loader=yaml.FullLoader)
            cfg = dacite.from_dict(data_class=StrategyConfig, data=conf)
    except Exception as e:
        print(f"failed to load config: {e}")
        exit(1)
    
    strategy = MACD(
        # base_config = cfg.base_config
        file_depth = cfg.file_depth,
        file_trade= cfg.file_trade,
        file_write = cfg.file_write,
        ob_level = cfg.ob_level,
        start_time = cfg.start_time,
        end_time = cfg.end_time,
        symbols = cfg.symbols, # TODO list of symbols
        strategy_name = cfg.strategy_name,
        market_data_type = cfg.market_data_type,
        market_type = cfg.market_type,
        n_tick = cfg.n_tick,
        time_limit = cfg.time_limit
    )

    try:
        asyncio.run(strategy.start_strategy())
    except KeyboardInterrupt:
        print("KeyboardInterrupt, shutdown strategy process...")
    finally:
        pass
        # asyncio.run(strategy._fini())