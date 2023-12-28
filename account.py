from instruction import Instruction
from position import Position
import Optional

class Account:
    def __init__(
            self,
            account_id: str,
            initial_cash = 0.0,
            commission_rate = 0.0001
    ):
        self.account_id = account_id
        self.hist_positions = Optional[list[Position]] = None
        self.hist_instruction = Optional[list[Instruction]] = None
        self.initial_cash = initial_cash
        self.commission_rate = commission_rate
        self.current_cash = self.initial_cash
        self.current_pnl = 0.0
        self.hist_pnl = Optional[list[float]] = None
        self.closing_all_position = False
    def get_current_cash(self) -> float:
        return self.current_cash
    def get_current_pnl(self) -> float:
        return self.current_pnl
    def create_instruction():
        """
        create new instruction according to the signal, added to the instruction list
        """
    def update_instruction():
    def cancell_instruction():
