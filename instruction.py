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
            direction: int,
            limit_price: float,
            time_limit: float,
            ts: int, # TODO 目前使用timestamp，是否转化为datetime格式, create instruction里也要修改
        ):
            self.instruction_id = uid  # unique id for this instruction
            self.quantity = quantity  # quantity of the instruction
            self.limit_price = limit_price  # limit price of the instruction
            self.direction = direction  # BUY:1 SELL:-1
            self.market_type = market_type  # spot, margin, uswap, ucontract
            self.symbol = symbol  # symbol of the instruction, currently only btcusdt
            self.current_time = self.begin_time()
            self.status = InstructionStatus(uid, ts, time_limit, quantity)
         

        async def update_inst(self, new_ts: int, entry_depth_qty: float):
            await self.status.update_instruction_status(new_ts, entry_depth_qty)

        async def execute_inst(self, execute_qty, execute_price) -> float:
            traded_vol = await self.status.execute_instructions(execute_qty, execute_price)
            self.tag == 'update'    
            return traded_vol   

        async def get_target_quantity(self) -> float:
            return self.quantity
        
        async def get_quantity_remain(self) -> float:    # 返回的是inst本身挂单量的剩余量
            return self.status.get_quantity_remain()
        
        async def get_limit_price(self) -> float:
            return self.limit_price
        
        async def get_entry_qty(self) -> float:
            return self.status.get_entry_qty()
        
        async def update_quantity_inst_priotized(self, new_value: float):
            self.status.update_quantity_inst_priotized(new_value)

        async def get_quantity_inst_priotized(self) -> float:
            return self.get_quantity_inst_priotized() 
        
        async def update_quantity_entry_priotized(self, new_value: float):
            self.status.update_quantity_entry_priotized(new_value)

        async def get_quantity_entry_priotized(self) -> float:
            return self.get_quantity_entry_priotized() 
        
        async def get_tag(self) -> str:
            return self.status.get_tag()
        
        async def get_direction(self) -> int:
            return self.direction
        
        async def is_created(self) -> bool:
            return self.status.is_created()
        
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
        self.created = False
        self.end_time = ts + time_limit * 1e9   # 秒为单位
        
        # self.entry_depth_qty = 0.0
        self.quantity_entry_priotized = 0.0  # TODO 在当前价格下优先级更高的qty，每次用trade数据吃挂单时先减这个qty，一直到0以后才减quantity_remain
        self.quantity_inst_priotized = 0.0

    async def update_instruction_status(self, new_ts: int, entry_depth_qty: float):
        self.current_time = new_ts
        if self.current_time >= self.end_time:
            self.status.time_exceeded=True
        if self.created == False:
            self.created == True
            self.quantity_entry_priotized = entry_depth_qty
        
    async def update_quantity_inst_priotized(self, new_value: float):
        self.quantity_inst_priotized = new_value

    async def get_quantity_inst_priotized(self) -> float:
        return self.quantity_inst_priotized 
    
    async def update_quantity_entry_priotized(self, new_value: float):
        self.quantity_entry_priotized = new_value

    async def get_quantity_entry_priotized(self) -> float:
        return self.quantity_entry_priotized 
    
    async def isExcceedTime(self) -> bool:
        return self.time_exceeded

    async def execute_instruction(self, execute_qty, execute_price) -> float:   # TODO 此处的execute qty是由trade data提供的
        remaining_qty = execute_qty - self.quantity_entry_priotized             # 先扣除quantity_entry_priotized，再扣除quantity_inst_priotized

        if remaining_qty <=0.0:
            self.quantity_entry_priotized -= execute_qty
            return execute_qty
        else:
            execute_qty -= self.quantity_entry_priotized
            remaining_qty = execute_qty - self.quantity_inst_priotized   
            if remaining_qty <= 0.0:
                self.quantity_inst_priotized -= execute_qty
                execute_qty += self.quantity_entry_priotized 
                self.quantity_entry_priotized = 0.0
                return execute_qty
            else:
                remaining_qty = min(self.quantity_remain, remaining_qty)
                execute_qty = self.quantity_entry_priotized + self.quantity_inst_priotized + remaining_qty
                self.quantity_entry_priotized = 0.0
                self.quantity_inst_priotized = 0.0
                self.quantity_remain -= remaining_qty
                self.quantity_traded += remaining_qty
                self.avg_price_traded = (self.quantity_traded * self.avg_price_traded + remaining_qty * execute_price) / (self.quantity_traded + remaining_qty)
                self.tag = 'update'   # only when qty traded will the tag be change to 'update'
                if self.quantity_remain == 0:
                    self.completed = True
                return execute_qty

    async def isCompleted(self) -> bool:
        return self.completed
    async def is_created(self) -> bool:
        return self.create
    async def get_quantity_remain(self) -> float:
        return self.quantity_remain

    async def get_tag(self) -> str:
        return self.tag
