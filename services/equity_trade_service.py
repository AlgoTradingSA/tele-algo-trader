import asyncio
import datetime
import logging
from asyncio import AbstractEventLoop
from math import floor
from time import sleep

from retry import retry

from services.shoonya_broker_service import ShoonyaBrokerService
from services.trade_service import TradeService, get_close_leg_type
from utils import db_utils, bot_utils
from utils.helpers import poller
from utils.models import SetQueue, ContractStatus, TradeCandidate, TradeChannels, TradeContract

logger = logging.getLogger(__name__)


class EquityTradeService(TradeService):

    def __init__(self, broker_service: ShoonyaBrokerService, loop: AbstractEventLoop, trade_config: dict):
        # do nothing
        super().__init__(broker_service, loop, trade_config)
        self.candidate_queue = SetQueue()
        self.potential_trades_dict = {}  # {ticker-name: buy-order-id}
        self.lock = asyncio.Lock()

    def order_update_listener(self, in_message):
        logger.info(f'Received broker update: {in_message}')
        order_id = in_message['norenordno']
        tsym = in_message['tsym']
        sleep(1)
        # with thread_lock:
        if order_id and order_id in self.potential_trades_dict.values():  # check only if buy order was placed
            tran_type = in_message['trantype']
            if in_message['status'] == 'COMPLETE':
                fill_price = in_message['flprc']

                msg = f"[{tran_type}] Order executed successfully for symbol {tsym}, fill price: [{fill_price}], qty: [{in_message['qty']}]"
                self.loop.create_task(bot_utils.send_alert(msg))

                self.loop.create_task(
                    self.gtt_order_handling_async(order_id, in_message['exch'], tsym, in_message['qty'], fill_price,
                                                  in_message['fltm']))

            elif in_message['status'] in ['REJECTED', 'INVALID_STATUS_TYPE', 'CANCELED']:
                self.potential_trades_dict[tsym] = None
                db_utils.delete_stock_record(order_id)
                msg = f"[{tran_type}] Order [{order_id}/{tsym}] failed: [{in_message['rejreason']}]"
                self.loop.create_task(bot_utils.send_alert(msg, logging.WARN))

    async def gtt_order_handling_async(self, order_id, exchange, tsym, qty, fill_prc=None, fill_time=None):
        async with self.lock:
            self.gtt_order_handling(order_id, exchange, tsym, qty, fill_prc, fill_time)

    def gtt_order_handling(self, order_id, exchange, tsym, qty, fill_prc=None, fill_time=None):
        sl, tgt = db_utils.get_sr_prices(order_id)
        is_ggt_placed, gtt_resp = self.place_cover_order('S', exchange, tsym, qty, tgt, sl)
        al_id = None
        if is_ggt_placed:
            msg = f'Successfully placed GTT order for [{tsym}/{order_id}]: {gtt_resp}'
            self.loop.create_task(bot_utils.send_alert(msg))
            status = ContractStatus.OPEN
            al_id = gtt_resp['al_id']
            self.open_trades.add(tsym)
            self.potential_trades_dict.pop(tsym, None)
        else:
            msg = f'Failed to place GTT order for [{tsym}/{order_id}]: {gtt_resp}'
            self.loop.create_task(bot_utils.send_alert(msg, logging.WARN))
            status = ContractStatus.GTT_PENDING
        db_utils.update_gtt_status(status, order_id, al_id, fill_prc, fill_time)
        db_utils.delete_trade_candidate(tsym)

    async def trade_selector(self):
        existing_trade_count = len(set(self.potential_trades_dict.values()) - {None}) + len(self.open_trades)
        if existing_trade_count >= self.trade_config['max_open_trades']:
            logger.info(f"Max trade limit of [{self.trade_config['max_open_trades']}] reached")
        elif not self.candidate_queue.empty():
            candidate: TradeCandidate = await self.candidate_queue.get()
            # logger.info(candidate)
            trade_count = db_utils.get_active_trade_count(TradeChannels[candidate.trade_channel])
            max_count = self.trade_config[candidate.trade_channel]['max_open_trades']
            if trade_count < max_count:
                asyncio.create_task(self.execute_trade(candidate))
            else:
                logger.info(f"Max trade limit of [{max_count}] reached for [{candidate.trade_channel}]")

    @retry(Exception, tries=3, delay=5)
    async def execute_trade(self, tc: TradeCandidate, order_type: str = None):
        quantity = floor(self.trade_config[tc.trade_channel]['fund_alloc'] / tc.cmp)
        cash = self.broker_service.get_available_cash()
        if cash < (quantity * tc.cmp):
            logger.warning(f"Balance of {[cash]} is insufficient to trade")
            return
        if quantity < 1:
            logger.warning(f"Unable to place trade for [{tc.trading_symbol}] with quantity: [{quantity}]")
            db_utils.delete_trade_candidate(tc.trading_symbol)
            self.potential_trades_dict.pop(tc.trading_symbol, None)
            return
        order_type = tc.open_leg
        msg = f"Placing [BUY] Order for symbol {tc.trading_symbol}, CMP: [{tc.cmp}], target: {tc.tgt}, " \
              f"quantity: {quantity}, stop_loss: {tc.sl}, channel: {tc.trade_channel}"
        await bot_utils.send_alert(msg)

        async with self.lock:
            res = self.broker_service.place_order(buy_or_sell=order_type, product_type='C', exchange=tc.exchange,
                                                  tradingsymbol=tc.trading_symbol, quantity=quantity, discloseqty=0,
                                                  price_type='LMT', price=tc.cmp, retention='DAY',
                                                  remarks=f'Buy order for {tc.trading_symbol}')

            if res is None:
                raise Exception(f"Failed to place order for symbol={tc.trading_symbol}")
            else:
                # print(res)
                await asyncio.sleep(0.1)
                order_id = res['norenordno']
                o_book = self.broker_service.get_order_book()

                for order in o_book:
                    if order_id == order['norenordno']:
                        if order['status'] == 'REJECTED':
                            msg = f"Order for [{tc.trading_symbol}] rejected by broker: {order['rejreason']}"
                            await bot_utils.send_alert(msg, logging.WARN)
                        else:
                            self.potential_trades_dict[tc.trading_symbol] = order_id
                            contract = TradeContract.from_trade_candidate(tc, ContractStatus.PENDING, quantity,
                                                                          order_id)
                            await db_utils.insert_stock_record(contract)
                            logger.info(f"Successfully placed order: {res}")
                        return
                logger.warning(f"Order {order_id} not found in order book")

    # TESTING METHOD
    async def gtt_testing(self, res, tc, order_id):
        logger.info("Testing here")
        res['status'] = 'COMPLETE'
        res['tsym'] = tc.trading_symbol
        res['trantype'] = 'B'
        res['rejreason'] = 'Testing'
        res['qty'] = 1
        res['flprc'] = tc.cmp
        res['exch'] = tc.exchange
        res['fltm'] = datetime.datetime.now()
        self.potential_trades_dict[tc.trading_symbol] = order_id
        contract = TradeContract.from_trade_candidate(tc, ContractStatus.PENDING, 1,
                                                      order_id)
        await db_utils.insert_stock_record(contract)
        self.order_update_listener(res)

    def pending_trade_handler(self, pending_ids: [] = None, exch: str = 'NSE', startup: bool = False):
        pending_ids = set(self.potential_trades_dict.values()) - {None}
        super().pending_trade_handler(pending_ids, exch, startup)

    async def pending_poller(self):
        logger.info("Polling for Pending orders")
        async with self.lock:
            self.pending_trade_handler()

    async def gtt_pending_poller(self):
        pending_contracts = await db_utils.get_gtt_pending_orders()
        logger.info(f"Polling for GTT pending orders, size=[{len(pending_contracts)}]")
        for contract in pending_contracts:
            await self.gtt_order_handling_async(contract.buy_id, contract.exchange, contract.trading_symbol,
                                                contract.qty)

    async def candidate_poller(self):
        async with self.lock:
            candidates_tsym = list(
                key for (key, value) in reversed(self.potential_trades_dict.items()) if key and not value)
            logger.info(f"Polled Candidate list: {candidates_tsym}")
            if candidates_tsym:
                candidates = db_utils.get_trade_candidates(candidates_tsym)
                logger.info(f"Polling for trade candidates, size=[{len(candidates)}]")
                for candidate in candidates:
                    # logger.info(candidate)
                    await self.candidate_queue.put(candidate)

    async def startup_activity(self):
        db_utils.purge_closed_orders()
        db_utils.purge_trade_candidates()
        tc = db_utils.get_trade_candidates()
        self.potential_trades_dict.update({t.trading_symbol: None for t in tc})
        pending_orders = db_utils.get_pending_orders()
        self.potential_trades_dict.update({p.trading_symbol: p.buy_id for p in pending_orders})
        logger.info(f"Initial Candidate list: {self.potential_trades_dict}")
        self.pending_trade_handler(True)

        self.open_trades.update([tc.trading_symbol for tc in db_utils.get_open_orders()])
        logger.info(f"Open Trades: {self.open_trades}")

    @poller()
    async def polling_activity(self):
        await self.pending_poller()
        await self.open_trade_handler('NSE')
        await self.gtt_pending_poller()
        await self.candidate_poller()
        await self.trade_selector()

    async def create_trade_candidate(self, tc_list: [TradeCandidate]):
        for tc in tc_list:
            async with self.lock:
                if tc and not (
                        tc.trading_symbol in self.potential_trades_dict or tc.trading_symbol in self.open_trades):
                    self.potential_trades_dict[tc.trading_symbol] = None
                    await self.candidate_queue.put(tc)
                    await db_utils.insert_trade_candidate(tc)
