import time
import orjson
from position import Position


class Instruction:
        TAG_OPENING = "opening"
        TAG_CLOSING = "closing"
        TAG_EMPTY = "empty"

        ACTION_CREATE = "CREATE"
        ACTION_UPDATE = "UPDATE"
        ACTION_CANCEL = "CANCEL"

        def __init__(
            self,
            uid: str,
            symbol: str,
            market_type: str,
            quantity: float,
            direction: str,
            limit_price: float,
            time_limit: float,
            ts: int, # TODO 目前使用timestamp，是否转化为datetime格式, create instruction里也要修改
        ):
            self.instruction_id = uid  # unique id for this instruction
            self.quantity = quantity  # quantity of the instruction
            self.limit_price = limit_price  # limit price of the instruction
            self.direction = direction  # BUY or SELL
            self.market_type = market_type  # spot, margin, uswap, ucontract
            self.symbol = symbol  # symbol of the instruction, currently only btcusdt
            self.current_time = self.begin_time()
            self.status = InstructionStatus(uid, ts, time_limit, quantity)


        async def update_inst(self, new_ts):
            await self.status.update_instruction_status(new_ts)

        async def excute_inst(self, excute_qty, excute_price):
            await self.status.excute_instructions(excute_qty, excute_price)
            self.tag == 'update'       

        async def get_target_quantity(self) -> float:
            return self.quantity
        
        async def get_quantity_remain(self) -> float:
            return self.status.get_quantity_remain()
        
        async def get_limit_price(self) -> float:
            return self.limit_price

        async def get_tag(self) -> str:
            return self.status.get_tag()
        
        def to_json(self):
            return orjson.dumps(self, default=lambda o: o.__dict__, option=orjson.OPT_SERIALIZE_NUMPY)


class InstructionStatus:
    """
    Instruction status, updated by the backtest engine.
    """

    def __init__(
        self,
        instruction_id: str,
        ts: int,
        time_limit: int,
        quantity_remain: float,
        quantity_traded: float = 0.0,
        completed: bool = False,
        time_exceeded: bool = False,
        avg_price_traded: float = 0.0,
    ):
        self.instruction_id = instruction_id
        self.completed = completed
        self.quantity_remain = quantity_remain
        self.quantity_traded = quantity_traded
        self.time_exceeded = time_exceeded
        self.avg_price_traded = avg_price_traded
        self.current_time = ts
        self.tag = 'opening'
        self.end_time = ts + time_limit * 1e3

    async def update_instruction_status(self, new_ts):
        self.current_time = new_ts
        if self.current_time >= self.end_time:
            self.status.time_exceeded=True

    async def isExcceedTime(self) -> bool:
        return self.time_exceeded

    async def excute_instruction(self, excute_qty, excute_price):
        self.quantity_remain -= excute_qty
        self.quantity_traded += excute_qty
        self.avg_price_traded = (self.quantity_traded * self.avg_price_traded + excute_qty * excute_price) / (self.quantity_traded + excute_qty)
        if self.quantity_remain == 0:
            self.completed = True
        self.tag = 'update'

    async def isCompleted(self) -> bool:
        return self.completed
    async def get_quantity_remain(self) -> float:
        return self.quantity_remain
    async def get_tag(self) -> str:
        return self.tag
