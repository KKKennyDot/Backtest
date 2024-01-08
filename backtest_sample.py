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
    
    async def transaction(self): 
        ts = self.market_data.order_book.ts
        for inst in self.instruction_pending: 
            cur_price = inst.get_limit_price()
            cur_direction = inst.get_direction()
            if cur_direction == 1: entry_qty = self.market_data.order_book.get_vol_from_price(cur_price, 'bid')
            else: entry_qty = self.market_data.order_book.get_vol_from_price(cur_price, 'ask')
            await inst.update_inst(ts, entry_qty)    # 如果没有这个价格档位，entry qty为0

            if inst.isCompleted():
                self.instruction_completed.append(inst)
                self.instruction_pending.remove(inst)
            if inst.isExcceedTime():  # if time excceed the limit
                self.instruction_cancelled.append(inst)
                continue

            # TODO 先挂单与更新
            if inst.direction == 1: # BUY
                if cur_price not in self.market_data.order_book.get_price_list('bid'):   # 不在depth price里的新价格，单独为一档
                    if cur_price not in self.priority_qty_per_Buyprice.keys():   # 在累加列表里第一次出现，优先级qty为0
                        # inst.update_quantity_entry_priotized(0.0)  
                        inst.update_quantity_inst_priotized(0.0)
                        self.priority_qty_per_Buyprice[cur_price] = inst.get_quantity_remain()
                        self.trade_bid_dict[cur_price] = [(inst.get_quantity_remain(), inst.uid)]
                    else:                                                        # 已经存在在累加列表里，前序有相同价位inst
                        # inst.update_quantity_entry_priotized(0.0) 
                        inst.update_quantity_inst_priotized(self.priority_qty_per_Buyprice[cur_price])  
                        self.priority_qty_per_Buyprice[cur_price] += inst.get_quantity_remain()
                        self.trade_bid_dict[cur_price].append((inst.get_quantity_remain(), inst.uid))

                else:                                    # 在depth price里的价格,此时有entry priotized qty
                    if cur_price not in self.priority_qty_per_Buyprice.keys():
                        # cur_vol = inst.get_entry_qty()
                        # cur_vol = self.market_data.order_book.get_vol_from_price(cur_price, 'bid')
                        inst.update_quantity_inst_priotized(0.0)
                        self.priority_qty_per_Buyprice[cur_price] = inst.get_quantity_remain()
                    else:
                        # cur_vol = inst.get_entry_qty()
                        # cur_vol = self.market_data.order_book.get_vol_from_price(cur_price, 'bid')
                        inst.update_quantity_inst_priotized(self.priority_qty_per_Buyprice[cur_price])
                        self.priority_qty_per_Buyprice[cur_price] += inst.get_quantity_remain()

                    self.trade_bid_dict[cur_price].append((inst.get_quantity_remain(), inst.uid))  # TODO 在该价格的qty list后面增添（vol，uid）

            else:  # SELL
                if cur_price not in self.market_data.order_book.get_price_list('ask'):   # 不在depth price里的新价格，单独为一档
                    if cur_price not in self.priority_qty_per_Sellprice.keys():
                        inst.update_quantity_inst_priotized(0.0)  
                        self.priority_qty_per_Sellprice[cur_price] = inst.get_quantity_remain()
                        self.trade_bid_dict[cur_price] = [(inst.get_quantity_remain(), inst.uid)]
                    else:
                        inst.update_quantity_inst_priotized(self.priority_qty_per_Sellprice[cur_price])  
                        self.priority_qty_per_Sellprice[cur_price] += inst.get_quantity_remain()
                        self.trade_bid_dict[cur_price].append((inst.get_quantity_remain(), inst.uid))
                else:                                    # 在depth price里的价格，分为第一个出现的以及后续价格档位的
                    if cur_price not in self.priority_qty_per_Sellprice.keys():
                        # cur_vol = self.market_data.order_book.get_vol_from_price(cur_price, 'ask')
                        inst.update_quantity_inst_priotized(0.0)
                        self.priority_qty_per_Sellprice[cur_price] = inst.get_quantity_remain()
                    else:
                        # cur_vol = self.market_data.order_book.get_vol_from_price(cur_price, 'ask')
                        inst.update_quantity_inst_priotized(self.priority_qty_per_Sellprice[cur_price])
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
                            execute_qty = min(vol_list[0][0], trade_vol)
                            trade_vol -= execute_qty
                            self.trade_bid_dict[price][0] = (self.trade_bid_dict[price][0][0] - execute_qty, self.trade_bid_dict[price][0][1])
                        elif vol_list[0][1] != 'depth' and len(vol_list) >= 1:     # 只有新价位的inst的vol（可以有多个inst） 
                            res_vol = 0
                            for vol in vol_list:
                                cur_inst = self.instruction_pending[self.find_instruction_from_uid(vol[1])]
                                res_vol = await cur_inst.execute_inst(trade_vol, cur_price)  # 用trade vol去吃每个单，每个单里
                                if res_vol == trade_vol:   # 全部吃完
                                    trade_vol = 0.0
                                    break
                            trade_vol -= res_vol
                        elif vol_list[0][1] == 'depth' and len(vol_list) > 1:      # 原本depth data里有vol，并且也有该价位的inst等待吃单
                            res_vol = 0
                            for vol in vol_list[1:]:
                                cur_inst = self.instruction_pending[self.find_instruction_from_uid(vol[1])]
                                res_vol = await cur_inst.execute_inst(trade_vol, cur_price)  # 用trade vol去吃每个单，每个单里
                                if res_vol == trade_vol:   # 全部吃完
                                    trade_vol = 0.0
                                    break
                            trade_vol -= res_vol
                    else: break

                for price, vol_list in self.trade_ask_dict.items():
                    if price <= trade_price and trade_vol > 0.0: 
                        if vol_list[0][1] == 'depth' and len(vol_list) == 1:       # 只有depth data里的vol 
                            execute_qty = min(vol_list[0][0], trade_vol)
                            trade_vol -= execute_qty
                            self.trade_ask_dict[price][0] = (self.trade_ask_dict[price][0][0] - execute_qty, self.trade_ask_dict[price][0][1])
                        elif vol_list[0][1] != 'depth' and len(vol_list) >= 1:     # 只有新价位的inst的vol（可以有多个inst） 
                            res_vol = 0
                            for vol in vol_list:
                                cur_inst = self.instruction_pending[self.find_instruction_from_uid(vol[1])]
                                res_vol = await cur_inst.execute_inst(trade_vol, cur_price)  # 用trade vol去吃每个单，每个单里
                                if res_vol == trade_vol:   # 全部吃完
                                    trade_vol = 0.0
                                    break
                            trade_vol -= res_vol
                        elif vol_list[0][1] == 'depth' and len(vol_list) > 1:      # 原本depth data里有vol，并且也有该价位的inst等待吃单
                            res_vol = 0
                            for vol in vol_list[1:]:
                                cur_inst = self.instruction_pending[self.find_instruction_from_uid(vol[1])]
                                res_vol = await cur_inst.execute_inst(trade_vol, cur_price)  # 用trade vol去吃每个单，每个单里
                                if res_vol == trade_vol:   # 全部吃完
                                    trade_vol = 0.0
                                    break
                            trade_vol -= res_vol
                    else: break
                    

    async def tick(self):

        cur_symbol = self.market_data.order_book.symbol
        cur_price = self.market_data.order_book.bids[0][0]   # TODO 用哪个价格来作为计算持仓价值的价格


        await self.account.update_position_value(cur_symbol, cur_price)  # 每天update仓位价值

        await self.transaction()   # TODO 顺序很重要，新发生的instruction下一tick才进行交易

        # signal and trading logic
   
        self.n_tick += 1  # TODO single symbol在每次tick后都增加1，但是多symbol处理情况不同
        # Signals
        # if macd > 0 and macd_prev < 0:
        self.account.directions[cur_symbol] = 1
            # self.account.macd_diff[cur_symbol] = macd-macd_prev
        # elif macd < 0 and macd_prev > 0:
        #     self.account.directions[cur_symbol] = -1
        #     self.account.macd_diff[cur_symbol] = macd_prev-macd
        # else:
        #     self.account.directions[cur_symbol] = 0
        #     self.account.macd_diff[cur_symbol] = 0
        
        return None

    async def post_signal(self):
        self.account.directions = dict.fromkeys(self.symbols, None)
        self.n_tick = 0

    async def is_signal_ready(self):
        print(self.n_tick)
        if len(self.symbols) == self.n_tick:
            
            return True
        else: return False
    
    async def signal_processing(self):
        qty = self.sizing_function(0.05)  # TODO 如何确定qty(0.5 for test right now)
        cur_ts = self.market_data.order_book.ts
        for symbol, direction in self.account.directions.items():
            if direction == 0:
                continue
            if direction == 1:
                # self.logger.info(f"Buy {symbol}")
                print('buy')
                inst = await self.account.create_instruction(
                    symbol = symbol, 
                    market_type = self.market_data.order_book.market_type, 
                    limit_price = self.market_data.order_book.bids[0][0],   # TODO 此挂单价需要自定义
                    qty = qty, 
                    direction = direction, 
                    time_limit = self.time_limit,
                    ts = cur_ts
                ) # TODO 改正函数格式
                self.instruction_pending.append(inst)
            elif direction == -1:
                # self.logger.info(f"Sell {symbol}")
                print('sell')
                inst = await self.account.create_instruction(
                    symbol = symbol, 
                    market_type = self.market_data.order_book.market_type, 
                    limit_price = self.market_data.order_book.asks[0][0],   # TODO 此挂单价需要自定义
                    qty = qty, 
                    direction = direction, 
                    time_limit = self.time_limit,  
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