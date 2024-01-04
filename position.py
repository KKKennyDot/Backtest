import time
import orjson

class Position:
    def __init__(
        self,
        symbol: str,
        ts: int,
        current_position: float = 0.0,
        target_position: float = 0.0,
        entry_price: float = 0.0,
    ):
        self.symbol = symbol
        self.current_position = current_position
        self.target_position = target_position
        self.entry_price = entry_price
        self.current_value = self.current_position * entry_price
        self.updated_at: float = 0.0 # default, to identify other value is just init or reset to zero value
        self.entry_ts = ts
        self.cost_basis = self.entry_price   # TODO 持仓成本的计算

    # async def set_current_position(self, new_value: float, new_price: float, ts: int):
    #     self.current_position = new_value
    #     self.current_value = new_price
    #     self.updated_at = ts
    
    # async def set_target_position(self, new_value: float, ts: int):
    #     self.target_position = new_value
    #     self.updated_at = ts

    async def update_current_value(self, current_price: float):
        self.current_value = self.current_position * current_price  # here do not update time
    
    async def update_symbol_position(self, traded_qty: float, target_qty: float, price: float, ts: int):
        if (traded_qty > 0.0 and self.current_position > 0.0) or (traded_qty < 0.0 and self.current_position < 0.0):    # TODO 卖出并不影响持仓成本(当交易方向与持仓方向一致时才会改变持仓成本)
            self.cost_basis = (self.cost_basis * self.current_position + traded_qty * price) / (self.current_position + traded_qty)
        self.current_position += traded_qty
        self.target_position += target_qty
        self.updated_at = ts
        
    async def get_current_value(self) -> float:
        return self.current_value


    def to_json(self):
        return orjson.dumps(self, default=lambda o: o.__dict__, option=orjson.OPT_SERIALIZE_NUMPY)

    def __repr__(self) -> str:
        return f"Symbol: {self.symbol}, Position: current: {self.current_position}, target: {self.target_position}, value: {self.current_value}, entry price: {self.entry_price}, updated at: {self.updated_at}"
    



    