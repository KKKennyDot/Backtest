import json
import time
import csv
import numpy as np
from collections import deque
from datetime import datetime
from abc import abstractmethod
from exchange.utils.strconv import base36_encode
from typing import Optional
from exchange.utils.logger import get_logger
import orjson


class MarketType:
    SPOT = "spot"
    MARGIN = "margin"
    USWAP = "uswap"
    UCONTRACT = "ucontract"

class Exchange:
    Binance = "binance"

class Position:
    def __init__(
        self,
        current_position: float = 0.0,
        entry_price: float = 0.0,
    ):
        self.current_position = current_position
        self.entry_price = entry_price

    def to_json(self):
        return orjson.dumps(self, default=lambda x: x.__dict__, option=orjson.OPT_SERIALIZE_NUMPY)

    def __repr__(self) -> str:
        return f"Position: current: {self.current_position}, entry price: {self.entry_price}"
    
class Instruction:
    TAG_OPENING = "opening"
    TAG_CLOSING = "closing"
    TAG_EMPTY = "empty"

    ACTION_CREATE = "CREATE"
    ACTION_UPDATE = "UPDATE"
    ACTION_CANCEL = "CANCEL"

    def __init__(
        self,
        trigger_id: str,
        uid: str,
        market_type: str,
        quantity: float,
        symbol: str,
        time_to_finish: int,
        trigger: dict,
        limit_price: float = None,
    ):
        self.trigger_id = trigger_id  # trigger id of the instruction
        self.instruction_id = uid  # unique id for this instruction
        self.quantity = quantity  # quantity of the instruction
        self.limit_price = limit_price  # limit price of the instruction
        self.market_type = market_type  # spot, margin, uswap, ucontract
        self.symbol = symbol  # symbol of the instruction
        self.time_to_finish = (
            time_to_finish  # time to finish the instruction in seconds
        )
        self.begin_time = time.time()  # begin time of the instruction
        self.end_time = time.time() + time_to_finish  # end time of the instruction
        self.traded_volume = 0.0  # traded volume of the instruction
        self.completed = False  # whether the instruction is completed
        self.trigger = trigger  # trigger of the instruction
        self.avg_price_traded = 0.0

    def to_json(self):
        return orjson.dumps(self, default=lambda o: o.__dict__, option=orjson.OPT_SERIALIZE_NUMPY)
    
    def __repr__(self) -> str:
        return f"Instruction quantity: {self.quantity}, Instruction limit price: {self.limit_price}"


class InstructionStatus:
    """
    Instruction status, fetched from matchmaking
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


class AggregationTrade:
    event_time: int
    recv_time: int
    symbol: str
    price: float
    quantity: float
    trade_time: int

    def __init__(
        self,
        event_time: int,
        symbol: str,
        price: float,
        quantity: float,
        trade_time: int,
    ):
        self.event_time = event_time
        self.symbol = symbol
        self.price = price
        self.quantity = quantity
        self.trade_time = trade_time
        self.recv_time = time.time_ns()


class Kline:
    def __init__(
        self,
        market_type: str,
        event_time: int,
        start_time: int,
        close_time: int,
        symbol: str,
        finished: bool,
        # deprecated, using volume instead
        quantity: float,
        open_price: float,
        close_price: float,
        high_price: float,
        low_price: float,
        interval: str,
        num_of_trades: int,
        quote_asset_volume: float,
        volume: float,
    ):
        self.market_type = market_type
        self.event_time = event_time
        self.start_time = start_time
        self.close_time = close_time
        self.symbol = symbol
        self.finished = finished
        self.open_price = open_price
        self.close_price = close_price
        self.high_price = high_price
        self.low_price = low_price
        self.quantity = quantity
        self.interval = interval
        self.recv_time = time.time_ns()
        self.num_of_trades = num_of_trades
        self.quote_asset_volume = quote_asset_volume
        self.volume = volume

    def to_json(self):
        return orjson.dumps(self, default=lambda o: o.__dict__)


class Orderbook:
    def __init__(self, market_type: str):
        self.market_type = market_type
        self.bids = deque()
        self.asks = deque()
        self.recv_ts = 0
        self.exch_ts = 0

class MarketData:
    def __init__(self):
        self.aggregation_trade: Optional[AggregationTrade] = None
        self.kline: Optional[Kline] = None
        self.order_book: Optional[Orderbook] = None

class BacktestBase:
    DEFAULT_MIN_NOTIONAL_VALUE = 100.0
    DEFAULT_QUANTITY_PRECISION = 0
    def __init__(
        self,
        strategy_name: str,  # strategy name
        cash: list[float],  # cash
        holding_worth: list[float],  # holding worth
        net_worth: list[float],  # net worth
        exchange: str,  # exchange
        log_path: str,  # path to log file
        symbols: list[str],  # list of symbols
        topics: dict,
        # whether the strategy is transactional trade, True when the instructions in one trigger are atomic
        transactional_instruction: bool = False,
        # test mode only detect signal, but not send instruction to execution engine
        # csv data to simulate websocket market data
        test_data_path: str = "",
    ):
        self.logger = get_logger(name=strategy_name, path=log_path)
        self.start_time = time.time()
        self.strategy_name = strategy_name
        self.cash = cash
        self.holding_worth = holding_worth
        self.net_worth = net_worth
        self.symbols = symbols
        self.position = dict.fromkeys(symbols)
        self.parameters = {}
        self.exchange = exchange
        self.markets = list[str]()
        self.topics = topics
        # Current tick market data
        self.market_data = MarketData()
        self.instructions = list[Instruction]()
        # Strategy position
        self.market_positions = dict[str, dict[str, Position]]()
        self.pnl = 0.0
        self._transactional_instruction = transactional_instruction
        self.closing_all_positions = False
        self.test_data_path = test_data_path
        if self.test_data_path == '':
            raise Exception("test_data_path is empty when test_mode is True")
        self._min_notional_values: dict[str, dict[str, float]] = {} # market_type, symbol in usdt
        self._quantity_precisions: dict[str, dict[str, int]] = {} # market_type, symbol, 3 for 0.001
        self._net_worth_value = 0.0
        self._last_tick_ts: dict[str, int] = {} # market_type, last tick timestamp

    async def start(self):
        await self.logger.init()
        await self.start_test()

    async def start_test(self):
        klines = await self._parse_csv_data()
        # iterate klines
        if len(klines) == 0:
            raise Exception("No kline data")
        for kline in klines:
            # override current kline
            self.market_data.kline = kline
            await self.before_tick()
            await self.tick()
            await self.post_tick()

            if await self.is_signal_ready():
                await self.signal_processing()
                await self.post_signal()

    async def change_position_inst(self, symbol, limit_price, qty):
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

    def generate_trigger_id(self) -> str:
        t = base36_encode(int(time.time() * 1000))  # convert to base36 to save space
        return f"{self.strategy_id}_{t}"

    async def _parse_csv_data(self):
        def _convert_or_nan(x, func):
            if x == np.nan:
                return np.nan
            try:
                return func(x)
            except:
                raise Exception(f"Failed to convert {x} to {func}")
            
        klines: list[Kline] = []  # list of Kline data, first element is start_time in ms
        # csv_column_name = ['format_trash', 'open_time', 'open', 'high', 'low', 'close',
        #    'volume', 'close_time', 'qav', 'num_trades',
        #    'taker_base_vol', 'taker_quote_vol', 'ignore', 'symbol', 'market_type', 'interval']
        with open(self.test_data_path, "r") as f:
            r = csv.reader(f)
            for row in r:
                row = [np.nan if x == '' else x for x in row]
                kline = Kline(
                    market_type=row[14],
                    symbol=row[13],
                    finished=True,
                    quantity=_convert_or_nan(row[6], float),
                    interval=row[15],
                    event_time=_convert_or_nan(row[1], int),
                    start_time=_convert_or_nan(row[1], int),
                    open_price=_convert_or_nan(row[2], float),
                    high_price=_convert_or_nan(row[3], float),
                    low_price=_convert_or_nan(row[4], float),
                    close_price=_convert_or_nan(row[5], float),
                    volume=_convert_or_nan(row[6], float),
                    close_time=_convert_or_nan(row[7], int),
                    quote_asset_volume=_convert_or_nan(row[8], float),
                    num_of_trades=_convert_or_nan(row[9], int),
                )
                klines.append(kline)

        return klines

    @abstractmethod
    async def instruction_updated(self, instruction: Instruction):
        """ Override this method to handle the instruction updated event callback

        Args:
            instruction (Instruction): inst instance
        """
        pass

    @abstractmethod
    async def tick(self):
        """
        Override this method to handle market data updates
        """
        pass

    @abstractmethod
    async def post_tick(self):
        pass

    @abstractmethod
    async def before_tick(self):
        pass

    @abstractmethod
    async def is_signal_ready(self) -> bool:
        """whether the message is enough to trigger the signal detection
            only when the message is enough, the strategy will call the multi_msg_signal_detection

        Returns:
            bool: result
        """
        return False

    @abstractmethod
    async def post_signal(self):
        pass

    @abstractmethod
    async def signal_processing(self):
        pass
    
