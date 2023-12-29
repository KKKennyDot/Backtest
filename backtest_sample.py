import asyncio
import numpy as np
from backtest import Backtest
from dataLoarder import (
    Orderbook,
    MarketData,
    orderbookLoader
)

import pandas as pd
# from filterpy.kalman import KalmanFilter
# from exchange.utils.data import get_kline_data
# from exchange.models import StrategyBaseConfig
from datetime import datetime
# from pytimeparse.timeparse import timeparse
import argparse
import yaml
import dacite

class MACD(Backtest):
    def __init__(
        self,
        n_tick: int,
    ):
        self.n_tick = n_tick
        self.p = {
            "interval": '8H',
            "short_length": 5,
            "long_length": 20,
            "max_position": 1.0,
            "sizing_coeff": 1.0 # TODO 这里sizing coeff是什么意思
        }

    async def init_strategy(self):
        self.symbols = ['btcusdt']

    # def _sizing_function(self, macd_diff: float):
    #     """init sizing function"""
    #     target_size = np.clip(self.p['sizing_coeff'] * raw_size, -1, 1)
    #     self.logger.debug(f"Raw size: {raw_size}; Scaled size: {target_size}")
    #     return target_size

    async def tick(self):
        cur_symbol = self.market_data.order_book.symbol

        

        # signal and trading logic
        macd = 10   # TODO 暂时是乱定义的，需要修改
        macd_prev = 9
        self.n_tick += 1
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
        self.directions = dict.fromkeys(self.symbols, None)
        self.n_tick = 0

    async def is_signal_ready(self):
        if len(self.symbols) == self.n_tick:
            return True
        return False
    
    async def signal_processing(self):
        # qty = self._sizing_function(self.directions)  # TODO 如何确定qty
        for symbol, direction in self.directions.items():
            if direction == 0:
                continue
            if direction == 1:
                self.logger.info(f"Buy {symbol}")
                self.account.change_position_inst(symbol=symbol, limit_price=None, qty=qty)
            elif direction == -1:
                self.logger.info(f"Sell {symbol}")
                self.account.change_position_inst(symbol=symbol, limit_price=None, qty=-qty)

    async def start_strategy(self):
        await self._init_strategy()
        await self.start()

    


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--f", type=str, default="conf/conf_ewma_filter.yaml"
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
        base_config=cfg.base_config,
        kf_path=cfg.strategy_config["kf_path"],
        using_latest_kf=cfg.strategy_config.get("using_latest_kf", False),
        market_type=cfg.strategy_config.get("market_type", "uswap"),
        symbol=cfg.strategy_config.get("symbol", "BTCUSDT"),
    )

    try:
        asyncio.run(strategy.start_strategy())
    except KeyboardInterrupt:
        print("KeyboardInterrupt, shutdown strategy process...")
    finally:
        asyncio.run(strategy._fini())