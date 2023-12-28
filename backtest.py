import dataProc

import orjson
from abc import abstractmethod
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
        conf: BackTestConfig,
    ) -> None:
        self.instruction_pending = Optional[list[Instruction]]
        self.instruction_completed = Optional[list[Instruction]]
        self.instruction_cancelled = Optional[list[Instruction]]
    def load_market_data(self) -> None:
        pass
    def _init_account(self) -> None:
        pass

    def signal():
        """
        decide whether buy/hold/sell
        """

    def tick(snapshot):
        buy_signal = signal()
        instuction = self.account.create_instruction()


    def pre_tick():
    def post_tick():


    def start_test(self) -> None:

        def _convert_or_nan(x, func):
            if x == np.nan:
                return np.nan
            try:
                return func(x)
            except:
                raise Exception(f"Failed to convert {x} to {func}")
            
        Orderbooks: list[Orderbook] =[]
        with open(file_depth, "r") as f:


    def get_position(
        self,
        market_type: str,
        symbol: str,
    ) -> Position:
        return self.market_positions.setdefault(market_type, dict[str, Position]()).setdefault(symbol, Position())

    def update_target_position(self, market_type: str, symbol: str, delta: float):
        """
        update target position
        """
        pos = self.get_position(market_type, symbol)
        pos.set_target_position(
            new_value=self.quantity_round_by_step_sz(
                market_type=market_type,
                symbol=symbol,
                quantity=pos.target_position + delta
            )
        )
        # self.market_positions[market_type][symbol] = pos

    def update_current_position(self, market_type: str, symbol: str, delta: float):
        """
        update current position
        """
        pos = self.get_position(market_type, symbol)
        pos.set_current_position(
            new_value=self.quantity_round_by_step_sz(
                market_type=market_type,
                symbol=symbol,
                quantity=pos.current_position + delta
            ) 
        )
        # self.market_positions[market_type][symbol] = pos

    # async def update_position(self, u: InstructionStatus):
    #     if u is None or u.quantity_traded == 0:
    #         return
    #     updated = False
    #     for i in self.instructions:
    #         if i.instruction_id == u.instruction_id:
    #             self.logger.debug(
    #                 f"update position by instruction: {u.quantity_traded}, "
    #                 f"instruction: {i.to_json()}, "
    #                 f"strategy position: {self.get_symbol_position(i.market_type, i.symbol)}"
    #             )
    #             updated = self.update_strategy_position(i, u)
    #             await self.instruction_updated(i)
    #             await self.add_instruction2_redis(i)
    #             break
    #     if not updated:
    #         return
    #     await self.save_position()

    # async def save_position(self):
    #     al = []
    #     for market_type, positions in self.market_positions.items():
    #         for symbol, position in positions.items():
    #             al.append(self.save_symbol_position(market_type, symbol, position))

    #     await asyncio.gather(*al)



class MarketType:
    SPOT = "spot"
    MARGIN = "margin"
    USWAP = "uswap"
    UCONTRACT = "ucontract"

class Orderbook:
    def __init__(self, market_type: str) -> None:
        self.market_type = market_type
        self.bids = deque()
        self.asks = deque()
        self.recv_ts = 0
        self.exch_ts = 0

class MarketData:
    def __init__(self):
        self.order_book: Optional[Orderbook] = None



    




