from instruction import Instruction
from position import Position


import base36
import time
from typing import Optional

class Account:
    def __init__(
            self,
            account_id: str,
            initial_cash = 10000.0,
            commission_rate = 0.0001,
    ):
        self.account_id = account_id
        self.instructionNum = 0
        self.positionNum = 0
        self.commission_rate = commission_rate
        self.position = dict[str, Position]()
        # self.hist_positions = dict[str, Position]()
        # self.hist_instruction = dict[str, Instruction]()
        self.initial_cash = initial_cash
        self.cash: Optional[list[float]]()
        self.pnl: Optional[list[float]]()
        self.holding_worth: Optional[list[float]]()
        self.net_worth: Optional[list[float]]()
        self.closing_all_position = False

        self.directions = dict[str, int]()

    async def get_current_cash(self) -> float:
        return self.cash[-1]
    async def get_current_pnl(self) -> float:
        return self.pnl[-1]
    async def get_current_holding_worth(self) -> float:
        return self.holding_worth[-1]
    async def get_current_net_worth(self) -> float:
        return self.net_worth[-1]
    
    async def get_current_position(
        self,
        symbol: str,
    ) -> Position:
        return self.position[symbol]
    
    async def generate_inst_id(self) -> str:
        t = base36.dumps(int(time.time() * 1000))  # convert to base36 to save space
        return f"{self.account_id}_{t}"

    async def create_instruction(self, symbol: str,  market_type:str, qty: float, direction: int, limit_price: float, time_limit: int, tag: int, ts: int) -> Instruction:
        inst = Instruction(
            uid = self.generate_inst_id(),
            symbol = symbol,
            market_type = market_type,  # TODO 这里的market type怎么传参
            quantity = qty,
            direction = direction,
            limit_price = limit_price,
            time_limit = time_limit,
            tag = tag,
            ts = ts,
        )
        return inst
    
    async def update_position(self, symbol: str, traded_qty: float, target_qty: float, price: float, ts: int, tag: str):
        if symbol not in self.position.keys():
            new_position = Position(
                symbol = symbol,
                ts = ts,
                current_position = traded_qty,
                target_position = target_qty,
                entry_price = price  # TODO 此处的entry price是单子被吃了以后得价格还是0.0
            )
            self.position[symbol] = new_position
        else:
            
            if tag == 'opening':
                self.position[symbol].update_symbol_position(
                    traded_qty = traded_qty, 
                    target_qty = target_qty, 
                    price = price,
                    ts = ts
                )
            elif tag == 'update':
                self.position[symbol].update_symbol_position(
                    traded_qty = traded_qty, 
                    target_qty = 0.0, 
                    price = price,
                    ts = ts
                )
            else:
                raise Exception('wrong tag type!')
    async def update_position_value(self, symbol: str,  current_price: float):
        self.position[symbol].update_current_value(current_price = current_price)
    
    async def update_metric(self):
        last_cash = self.cash
        last_holding_worth = self.holding_worth
        self.holding_worth = 0.0
        last_net_worth = self.net_worth
        for symbol, pos in self.position.items():
            self.cash -= pos.current_value
            self.holding_worth += pos.current_value

        self.net_worth = self.cash + self.holding_worth
        self.pnl = self.net_worth - last_net_worth

    
    # async def change_position_inst(self, symbol: str, limit_price: float, qty: float):
    #     """open position
    #     Args:
    #         instruction (Instruction): instruction
    #     """
    #     if symbol not in self.position:
    #         self.position[symbol] = 0.0
    #     self.position[symbol] += qty
    #     self.holding_worth.append(qty * limit_price + self.holding_worth[-1])
    #     self.cash.append(qty * limit_price + self.cash[-1])
    #     self.net_worth.append(self.cash[-1] + self.holding_worth[-1])
    #     self.logger.info(f"Cur position: {self.position}")
    #     return 
    
    # # Change position based on the vwap price: 
    # # If next close > vwap, we can execute it successfully; 
    # # otherwise, we will fail to execute it
    # async def change_position_vwap(self, symbol, limit_price, qty):
    #     pass


    # def update_current_position(self, market_type: str, symbol: str, delta: float):
    #     """
    #     update current position
    #     """
    #     pos = self.get_position(market_type, symbol)
    #     pos.set_current_position(
    #         new_value=self.quantity_round_by_step_sz(
    #             market_type=market_type,
    #             symbol=symbol,
    #             quantity=pos.current_position + delta
    #         ) 
    #     )
    #     # self.market_positions[market_type][symbol] = pos

