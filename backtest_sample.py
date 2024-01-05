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
    
    # async def cal_available_vol(self, price):   # 需要trade数据做支撑
    #     available_vol = 0
    #     for trade_data in self.trade_list:
    #         if trade_data[0] <= price:
    #             available_vol += trade_data[1]
    #     return available_vol    
    
    async def transaction(self): 
        ts = self.market_data.order_book.ts
        for inst in self.instruction_pending: 
            cur_price = inst.get_limit_price()
            cur_direction = inst.get_direction()
            if cur_direction == 1: entry_qty = self.market_data.order_book.get_vol_from_price(cur_price, 'bid')
            else: entry_qty = self.market_data.order_book.get_vol_from_price(cur_price, 'ask')
            await inst.update_inst(ts, entry_qty)

            if inst.isCompleted():
                self.instruction_completed.append(inst)
                self.instruction_pending.remove(inst)
            if inst.isExcceedTime():  # if time excceed the limit
                self.instruction_cancelled.append(inst)
                continue

            # TODO 先挂单与更新
            if inst.direction == 1: # BUY
                if cur_price not in self.market_data.order_book.get_price_list('bid'):   # 不在depth price里的新价格，单独为一档
                    if cur_price not in self.priority_qty_per_Buyprice.keys():
                        inst.update_quantity_priotized(0.0)  
                        self.priority_qty_per_Buyprice[cur_price] = inst.get_quantity_remain()
                    else:
                        inst.update_quantity_priotized(self.priority_qty_per_Buyprice[cur_price])  
                        self.priority_qty_per_Buyprice[cur_price] += inst.get_quantity_remain()
                else:                                    # 在depth price里的价格，分为第一个出现的以及后续价格档位的
                    if cur_price not in self.priority_qty_per_Buyprice.keys():
                        cur_vol = inst.get_entry_qty()
                        # cur_vol = self.market_data.order_book.get_vol_from_price(cur_price, 'bid')
                        inst.update_quantity_priotized(cur_vol)
                        self.priority_qty_per_Buyprice[cur_price] = inst.get_quantity_remain()
                    else:
                        cur_vol = inst.get_entry_qty()
                        # cur_vol = self.market_data.order_book.get_vol_from_price(cur_price, 'bid')
                        inst.update_quantity_priotized(self.priority_qty_per_Buyprice[cur_price] + cur_vol)
                        self.priority_qty_per_Buyprice[cur_price] += inst.get_quantity_remain()

                self.trade_bid_dict[cur_price].append((inst.get_quantity_remain(), inst.uid))  # TODO 在该价格的qty list后面增添（vol，uid）

            else:  # SELL
                if cur_price not in self.market_data.order_book.get_price_list('ask'):   # 不在depth price里的新价格，单独为一档
                    if cur_price not in self.priority_qty_per_Sellprice.keys():
                        inst.update_quantity_priotized(0.0)  
                        self.priority_qty_per_Sellprice[cur_price] = inst.get_quantity_remain()
                    else:
                        inst.update_quantity_priotized(self.priority_qty_per_Sellprice[cur_price])  
                        self.priority_qty_per_Sellprice[cur_price] += inst.get_quantity_remain()
                else:                                    # 在depth price里的价格，分为第一个出现的以及后续价格档位的
                    if cur_price not in self.priority_qty_per_Sellprice.keys():
                        cur_vol = self.market_data.order_book.get_vol_from_price(cur_price, 'ask')
                        inst.update_quantity_priotized(cur_vol)
                        self.priority_qty_per_Sellprice[cur_price] = inst.get_quantity_remain()
                    else:
                        cur_vol = self.market_data.order_book.get_vol_from_price(cur_price, 'ask')
                        inst.update_quantity_priotized(self.priority_qty_per_Sellprice[cur_price] + cur_vol)
                        self.priority_qty_per_Sellprice[cur_price] += inst.get_quantity_remain()

                self.trade_ask_dict[cur_price].append((inst.get_quantity_remain(), inst.uid))  # TODO 在该价格的qty list后面增添（vol，uid）

        if self.trade_list != None:
            for trade_data in self.trade_list:
                trade_price = trade_data[0]
                trade_vol = trade_data[1]
                
                # TODO 检查depth数据，看是否能够吃掉，分别检查bid 和 ask侧的数据，看能吃掉多少
                for price, vol_list in self.trade_bid_dict.items():
                    if price >= trade_price and trade_vol > 0.0: 
                        if vol_list[0][1] == 'depth' and len(vol_list) == 1:       # 只有depth data里的vol 
                            trade_vol -= min(vol_list[0][0], trade_vol)
                        elif vol_list[0][1] != 'depth' and len(vol_list) >= 1:     # 只有新价位的inst的vol（可以有多个inst） 
                            res_vol = trade_vol
                            for vol in vol_list:
                                cur_inst = self.find_instruction_from_uid(vol[1])
                                res_vol -= await cur_inst.execute_inst(trade_vol, cur_price)  # 用trade vol去吃每个单，每个单里
                                if res_vol == 0: 
                                    trade_vol = res_vol
                                    break
                        elif vol_list[0][1] == 'depth' and len(vol_list) > 1:      # 原本depth data里有vol，并且也有该价位的inst等待吃单
                            res_vol = trade_vol
                            for vol in vol_list[1:]:  
                                cur_inst = self.find_instruction_from_uid(vol[1])
                                res_vol -= await cur_inst.execute_inst(trade_vol, cur_price)  # 用trade vol去吃每个单，每个单里
                                if res_vol == 0:                                        # TODO 注意这里是每次都是用trade vol去作为execute qty，因为每个inst里都记录的是优先级更高的所有vol
                                    trade_vol = res_vol
                                    break
                    else: break

                for price, vol_list in self.trade_bid_dict.items():
                    if price >= trade_price and trade_vol > 0.0: 
                        if vol_list[0][1] == 'depth' and len(vol_list) == 1:       # 只有depth data里的vol 
                            trade_vol -= min(vol_list[0][0], trade_vol)
                        elif vol_list[0][1] != 'depth' and len(vol_list) >= 1:     # 只有新价位的inst的vol（可以有多个inst） 
                            res_vol = trade_vol
                            for vol in vol_list:
                                cur_inst = self.find_instruction_from_uid(vol[1])
                                execute_qty = await cur_inst.execute_inst(trade_vol, cur_price)  # 用trade vol去吃每个单，每个单里
                                res_vol -= execute_qty
                                await self.account.update_position(
                                    symbol = inst.symbol, 
                                    traded_qty = execute_qty, 
                                    target_qty = inst.get_target_quantity(), 
                                    price = cur_price, 
                                    ts = ts, 
                                    tag = inst.get_tag()
                                )
                                if res_vol == 0: 
                                    trade_vol = res_vol
                                    break
                        elif vol_list[0][1] == 'depth' and len(vol_list) > 1:      # 原本depth data里有vol，并且也有该价位的inst等待吃单
                            res_vol = trade_vol
                            for vol in vol_list[1:]:  
                                cur_inst = self.find_instruction_from_uid(vol[1])
                                execute_qty = await cur_inst.execute_inst(trade_vol, cur_price)  # 用trade vol去吃每个单，每个单里
                                res_vol -= execute_qty
                                await self.account.update_position(
                                    symbol = inst.symbol, 
                                    traded_qty = execute_qty,   # TODO 此处有正负
                                    target_qty = inst.get_target_quantity(), 
                                    price = cur_price, 
                                    ts = ts, 
                                    tag = inst.get_tag()
                                )
                                if res_vol == 0:                                        # TODO 注意这里是每次都是用trade vol去作为execute qty，因为每个inst里都记录的是优先级更高的所有vol
                                    trade_vol = res_vol
                                    break
                    else: break
                    

    async def tick(self):
        cur_ts = self.market_data.order_book.ts
        cur_symbol = self.market_data.order_book.symbol
        cur_price = self.market_data.order_book.bids[0][0]   # TODO 用哪个价格来作为计算持仓价值的价格


        await self.account.update_position_value(cur_symbol, cur_price)  # 每天update仓位价值

        await self.transaction(cur_ts, cur_vol)   # TODO 顺序很重要，新发生的instruction下一tick才进行交易

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
                    symbol = symbol, 
                    market_type = self.market_data.order_book.market_type, 
                    limit_price = self.market_data.order_book.bids[0],   # TODO 此挂单价需要自定义
                    quantity = qty, 
                    direction = direction, 
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
                    limit_price = self.market_data.order_book.bids[0],   # TODO 此挂单价需要自定义
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