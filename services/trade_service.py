import logging
from asyncio import AbstractEventLoop
from typing import Any

from retry import retry

from services.shoonya_broker_service import ShoonyaBrokerService
from utils import db_utils, bot_utils
from utils.models import TradeContract, TradeCandidate, ContractStatus

logger = logging.getLogger(__name__)


def compute_tgt(cmp: float, tgt: float):
    default_tgt = round(1.1 * cmp, 2)
    if not tgt:
        return default_tgt
    elif tgt > 1.15 * cmp:
        return default_tgt
    elif tgt <= cmp:
        return default_tgt
    else:
        return tgt


def compute_sl(cmp: float, sl: float):
    default_sl = round(0.8 * cmp, 2)
    if not sl:
        return default_sl
    elif sl < 0.6 * cmp:
        return default_sl
    elif sl >= cmp:
        return default_sl
    else:
        return sl


def compute_long_derivative_exits(cmp, tgt, sl):
    new_tgt = 1.3 * cmp
    new_sl = 0.7 * sl

    if sl > new_sl:
        new_sl = sl
    if tgt < new_tgt:
        new_tgt = tgt

    return new_tgt, new_sl


def get_first_number(msg: list[str], default=None):
    result = default
    for m in msg:
        m = m.strip()
        try:
            result = float(m)
            break
        except:
            pass
    return result


def get_close_leg_type(open_leg_type: str) -> str:
    if open_leg_type == 'B':
        return 'S'
    else:
        return 'B'


class TradeService:
    def __init__(self, broker_service: ShoonyaBrokerService, loop: AbstractEventLoop, trade_config: dict):
        self.open_trades = set()
        self.broker_service = broker_service
        self.loop = loop
        self.trade_config = trade_config

    async def get_tsym_and_exch(self, name: str, exchange='NSE') -> (str, str):
        if name:
            json = self.broker_service.searchscrip(exchange, name)
            if json:
                logger.debug(json['values'])
                return json['values'][0]['tsym'], json['values'][0]['exch']
        return None, None

    async def get_token(self, name: str, exchange='NSE') -> Any | None:
        if name:
            json = self.broker_service.searchscrip(exchange, name)
            if json:
                logger.debug(json['values'])
                return json['values'][0]['token']
        return None

    def place_cover_order(self, buy_or_sell, exchange, trading_symbol, quantity, tgt, sl):
        resp = self.broker_service.place_oco_order(buy_or_sell, exchange, trading_symbol, quantity, tgt, sl)
        if resp and resp['stat'].lower() == 'oi created':
            return True, resp
        return False, resp

    def pending_trade_handler(self, pending_ids: [], exch, startup: bool = False):

        if len(pending_ids) > 0:
            trade_book = self.broker_service.get_trade_book()
            if trade_book:
                for trade in trade_book:
                    if trade['norenordno'] in pending_ids:
                        trade['status'] = 'COMPLETE'  # trade book always has completed trades
                        pending_ids.remove(trade['norenordno'])
                        self.order_update_listener(trade)
        if len(pending_ids) > 0:
            order_book = self.broker_service.get_order_book()
            if order_book:
                for order in order_book:
                    if order['norenordno'] in pending_ids:
                        pending_ids.remove(order['norenordno'])
                        self.order_update_listener(order)
        if len(pending_ids) > 0 and startup:
            db_utils.delete_stock_record(pending_ids, exch)

    async def open_trade_handler(self, exch):
        contracts = db_utils.get_open_orders(exch)
        logger.info(f"Reconciling OPEN [{exch}] orders, size=[{len(contracts)}]")

        trade_book = self.broker_service.get_trade_book()
        order_book = self.broker_service.get_order_book()
        trade_bk_dict, order_bk_dict = {}, {}
        if trade_book:
            trade_bk_dict = {(trade['tsym'], trade['trantype'], int(trade['qty'])): trade for trade in
                             reversed(trade_book)}  # ascending order by time
        if order_book:
            order_bk_dict = {(order['tsym'], order['trantype'], int(order['qty'])): order for order in
                             reversed(order_book)}

        for contract in contracts:
            is_processed = False
            close_leg = get_close_leg_type(contract.open_leg)
            if (contract.trading_symbol, close_leg, contract.qty) in trade_bk_dict:
                trade = trade_bk_dict[(contract.trading_symbol, close_leg, contract.qty)]

                contract.status = str(ContractStatus.CLOSE)
                if contract.open_leg == 'B':
                    contract.sell_price = trade['flprc']
                    contract.sell_id = trade['norenordno']
                    contract.sell_timestamp = trade['fltm']
                else:
                    contract.buy_price = trade['flprc']
                    contract.buy_id = trade['norenordno']
                    contract.buy_timestamp = trade['fltm']

                if exch != 'NFO':
                    self.open_trades.discard(contract.trading_symbol)

                msg = f'Closed order successfully for [{contract.trading_symbol}/{contract.buy_id}]'
                db_utils.delete_stock_record(contract.buy_id, exch)
                await db_utils.insert_stock_record(contract, exch)
                await bot_utils.send_alert(msg)
                is_processed = True
            if not is_processed and (contract.trading_symbol, close_leg, contract.qty) in order_bk_dict:
                order = order_bk_dict[(contract.trading_symbol, close_leg, contract.qty)]
                if order['status'] in ['REJECTED', 'INVALID_STATUS_TYPE', 'CANCELED']:
                    msg = f"[CLOSE Order [{order['tsym']}] failed: [{order['status']}] [{order['rejreason']}]"
                    await bot_utils.send_alert(msg, logging.WARN)

                    if exch != 'NFO':
                        contract.status = str(ContractStatus.GTT_PENDING)
                        db_utils.delete_stock_record(contract.buy_id, exch)
                        await db_utils.insert_stock_record(contract, exch)
                    else:
                        self.order_update_listener(order)

    def order_update_listener(self, in_message):
        raise NotImplementedError

    async def execute_trade(self, tc: TradeCandidate | TradeContract, order_type: str):
        raise NotImplementedError

    async def startup_activity(self):
        raise NotImplementedError

    async def polling_activity(self):
        raise NotImplementedError
