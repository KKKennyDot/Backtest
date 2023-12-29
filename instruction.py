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
            market_type: str,
            quantity: float,
            direction: str,
            symbol: str,
            time_to_finish: float,
            time_limit: float,
            position: Position,
            tag: str,
            limit_price: float = None,
        ):
            self.instruction_id = uid  # unique id for this instruction
            self.quantity = quantity  # quantity of the instruction
            self.limit_price = limit_price  # limit price of the instruction
            self.direction = direction  # BUY or SELL
            self.market_type = market_type  # spot, margin, uswap, ucontract
            self.symbol = symbol  # symbol of the instruction, currently only btcusdt
            self.tag = tag  # opening or closing or empty
            self.time_to_finish = (
                time_to_finish  # time to finish the instruction in seconds
            )
            self.begin_time = time.time()  # begin time of the instruction
            self.end_time = time.time() + time_limit  # end time of the instruction
            self.traded_volume = 0.0  # traded volume of the instruction
            self.avg_price_traded = 0.0 # avg price for the traded volumne of this instruction
            self.completed = False  # whether the instruction is completed
            self.status = InstructionStatus(uid)

            self.position = position  # for the update of position if the instruction excuted

        def if_exceed_time(self):
            current_time = time.time()
            if current_time >= self.end_time:
                 self.status.time_exceeded=True
                 return True
            else: return False


        def to_json(self):
            return orjson.dumps(self, default=lambda o: o.__dict__, option=orjson.OPT_SERIALIZE_NUMPY)


class InstructionStatus:
    """
    Instruction status, updated by the backtest engine.
    """

    def __init__(
        self,
        instruction_id: str,
        completed: bool = False,
        quantity_traded: float = 0.0,
        time_exceeded: bool = False,
        avg_price_traded: float = 0.0,
    ):
        self.instruction_id = instruction_id
        self.completed = completed
        self.quantity_traded = quantity_traded
        self.time_exceeded = time_exceeded
        self.avg_price_traded = avg_price_traded

