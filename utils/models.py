import logging
from asyncio import Queue
from enum import Enum
from datetime import datetime

logger = logging.getLogger(__name__)


def auto_str(cls):
    def __str__(self):
        return '%s(%s)' % (
            type(self).__name__,
            ', '.join('%s=%s' % item for item in vars(self).items())
        )

    def __repr__(self):
        return '%s(%s)' % (
            type(self).__name__,
            ', '.join('%s=%s' % item for item in vars(self).items())
        )

    cls.__str__ = __str__
    cls.__repr__ = __repr__
    return cls


class ContractStatus(Enum):
    OPEN = 1
    CLOSE = 2
    PENDING = 3
    GTT_PENDING = 4


class TradeChannels(Enum):
    equity99 = 1
    AngelOneAdvisory = 2
    CustomChannel = 3


@auto_str
class TradeCandidate:

    def __init__(self, trading_symbol: str, exchange: str, cmp: float, tgt: float, sl: float, open_leg: str,
                 trade_channel: TradeChannels, created_timestamp: datetime = None):
        self.trading_symbol = trading_symbol
        self.exchange = exchange
        self.cmp = cmp
        self.tgt = tgt
        self.sl = sl
        self.open_leg = open_leg

        if isinstance(trade_channel, str):
            if "." in trade_channel:
                self.trade_channel = str(trade_channel).split(".")[1]
            else:
                self.trade_channel = trade_channel
        else:
            self.trade_channel = trade_channel.name
        if created_timestamp:
            self.created_timestamp = created_timestamp
        else:
            self.created_timestamp = datetime.now()

    def __eq__(self, other):
        if isinstance(other, TradeCandidate):
            return self.trading_symbol == other.trading_symbol
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.__repr__())


@auto_str
class TradeContract(TradeCandidate):

    def __init__(self, trading_symbol: str, exchange: str, cmp: float, tgt: float, sl: float, open_leg: str,
                 trade_channel: TradeChannels, status: ContractStatus,
                 qty: int, buy_id: int, sell_id: int = None, gtt_al_id: int = None, buy_price: float = 0.0, sell_price: float = 0.0,
                 buy_timestamp: datetime = None, sell_timestamp: datetime = None, created_timestamp: datetime = None):
        super().__init__(trading_symbol, exchange, cmp, tgt, sl, open_leg, trade_channel, created_timestamp)

        self.status = str(status)
        self.qty = qty
        self.buy_id = buy_id
        self.sell_id = sell_id
        self.gtt_al_id = gtt_al_id
        self.buy_price = buy_price
        self.sell_price = sell_price
        self.buy_timestamp = buy_timestamp
        self.sell_timestamp = sell_timestamp

    @classmethod
    def from_trade_candidate(cls, tc: TradeCandidate, status: ContractStatus,
                             qty: int, buy_id: int):
        return cls(tc.trading_symbol, tc.exchange, tc.cmp, tc.tgt, tc.sl, tc.open_leg, tc.trade_channel,
                   status, qty, buy_id)


class SetQueue(Queue):

    def _init(self, maxsize=0):
        Queue._init(self, maxsize)
        self._queue = set()

    def _put(self, item):
        self._queue.add(item)

    def _get(self):
        return self._queue.pop()
