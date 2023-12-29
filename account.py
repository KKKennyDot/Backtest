from instruction import Instruction
from position import Position
import Optional

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
        self.hist_positions = Optional[dict(list[Position])] = None
        self.hist_instruction = Optional[dict(list[Instruction])] = None
        self.initial_cash = initial_cash
        self.cash = Optional[[float]] = None
        self.pnl = Optional[list[float]] = None
        self.holding_worth = Optional[list[float]] = None
        self.net_worth = Optional[list[float]] = None
        self.closing_all_position = False

        self.directions = Optional[dict(int)] = None

    def get_current_cash(self) -> float:
        return self.cash[-1]
    def get_current_pnl(self) -> float:
        return self.pnl[-1]
    def get_current_holding_worth(self) -> float:
        return self.holding_worth[-1]
    def get_current_net_worth(self) -> float:
        return self.net_worth[-1]
    
    def get_current_position(
        self,
        symbol: str,
    ) -> Position:
        return self.hist_positions[symbol][-1]

    def create_instruction(self, symbol: str, limit_price: float, qty: float) -> Instruction:

    
    async def change_position_inst(self, symbol: str, limit_price: float, qty: float):
        """open position
        Args:
            instruction (Instruction): instruction
        """
        if symbol not in self.position:
            self.position[symbol] = 0.0
        self.position[symbol] += qty
        self.holding_worth.append(qty * limit_price + self.holding_worth[-1])
        self.cash.append(qty * limit_price + self.cash[-1])
        self.net_worth.append(self.cash[-1] + self.holding_worth[-1])
        self.logger.info(f"Cur position: {self.position}")
        return 
    
    # Change position based on the vwap price: 
    # If next close > vwap, we can execute it successfully; 
    # otherwise, we will fail to execute it
    async def change_position_vwap(self, symbol, limit_price, qty):
        pass


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

