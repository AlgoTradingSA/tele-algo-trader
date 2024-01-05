import asyncio
import datetime
import logging
import random
from asyncio import AbstractEventLoop
from math import floor
from time import sleep
from zoneinfo import ZoneInfo

from retry import retry

from services.shoonya_broker_service import ShoonyaBrokerService
from services.trade_service import TradeService, get_close_leg_type
from utils import db_utils, bot_utils
from utils.helpers import get_lot_size, MARKET_CLOSE_TIME, IST_ZONE, get_current_time, poller
from utils.models import TradeCandidate, TradeChannels, ContractStatus, TradeContract, SetQueue

logger = logging.getLogger(__name__)


class DerivativeTradeService(TradeService):

    def __init__(self, broker_service: ShoonyaBrokerService, loop: AbstractEventLoop, trade_config: dict):
        super().__init__(broker_service, loop, trade_config)
        self.lock = asyncio.Lock()
        self.settlement_queue = SetQueue()
        self.immediate_long_exit_set = set()
        self.immediate_short_exit_set = set()

    async def startup_activity(self):
        logger.info("Performing startup activity for Derivative service")
        self.pending_trade_handler(True)
        await self.open_trade_handler('NFO')

    async def polling_activity(self):
        self.loop.create_task(self.settlement_poller())
        self.loop.create_task(self.pending_orders_poller())

    @poller(5)
    async def settlement_poller(self):
        if not self.settlement_queue.empty():
            contract_id = await self.settlement_queue.get()
            logger.info(f"Starting settlement for contract: [{contract_id}]")
            self.loop.create_task(self.settlement_processor(contract_id))

    @poller(60)
    async def pending_orders_poller(self):
        logger.info("Polling for Derivative Pending orders")
        self.pending_trade_handler()

    def pending_trade_handler(self, pending_ids: [] = None, exch: str = 'NFO', startup: bool = False):
        pending_ids = []
        pending_contracts: [TradeContract] = db_utils.get_pending_orders(exch)

        for ctx in pending_contracts:
            if ctx.open_leg == 'B':
                pending_ids.append(ctx.buy_id)
            else:
                pending_ids.append(ctx.sell_id)
        super().pending_trade_handler(pending_ids, exch, startup)

    async def prepare_trade(self, candidate: TradeCandidate):
        active_contracts = await db_utils.get_active_contracts_by_symbol(candidate.trading_symbol, candidate.open_leg,
                                                                         candidate.exchange)
        if active_contracts:
            logger.info(f"Active contract already present for symbol {candidate.trading_symbol}")
            return
        existing_trade_count = db_utils.get_active_trade_count(exch=candidate.exchange)
        if existing_trade_count >= self.trade_config['max_open_trades']:
            logger.info(f"Max trade limit of [{self.trade_config['max_open_trades']}] reached")
        else:
            trade_count = db_utils.get_active_trade_count(TradeChannels[candidate.trade_channel],
                                                          exch=candidate.exchange)
            max_count = self.trade_config[candidate.trade_channel]['max_open_trades']
            if trade_count < max_count:
                res = await self.execute_trade(candidate, candidate.open_leg)
                if res:
                    contract = TradeContract.from_trade_candidate(candidate, ContractStatus.PENDING, res['qty'],
                                                                  res['norenordno'])
                    await db_utils.insert_stock_record(contract, exch=candidate.exchange)
            else:
                logger.info(f"Max trade limit of [{max_count}] reached for [{candidate.trade_channel}]")

    @retry(Exception, tries=3, delay=5)
    async def execute_trade(self, tc: TradeCandidate | TradeContract, order_type: str):
        is_dry_run = self.trade_config['dry_run']
        if tc.open_leg == order_type:
            quantity = floor(self.trade_config[tc.trade_channel]['lot_count'] * get_lot_size(tc.trading_symbol))
            cash = self.broker_service.get_available_cash()
            if is_dry_run:
                cash += self.trade_config['dry_cash']
            if cash < (quantity * tc.cmp):
                logger.warning(f"Balance of {[cash]} is insufficient to trade")
                return
            if quantity < 1:
                logger.warning(f"Unable to place trade for [{tc.trading_symbol}] with quantity: [{quantity}]")
                return
            price = tc.cmp
            price_type = 'MKT'  # 'LMT'
            msg = f"Placing [{order_type}] Order for Option {tc.trading_symbol}, CMP: [{tc.cmp}], target: {tc.tgt}, " \
                  f"quantity: {quantity}, stop_loss: {tc.sl}, channel: {tc.trade_channel}"
        else:
            quantity = tc.qty
            price_type = 'MKT'
            price = '0.0'
            msg = f"Placing CLOSE LEG: [{order_type}] Order for Option {tc.trading_symbol}"
        await bot_utils.send_alert(msg)

        if is_dry_run:
            res = await self.construct_dry_run_order_response(tc, order_type, quantity)
            self.loop.create_task(self.handle_dry_run_order_update(res))
            return res

        async with self.lock:
            res = self.broker_service.place_order(buy_or_sell=order_type, product_type='M', exchange=tc.exchange,
                                                  tradingsymbol=tc.trading_symbol, quantity=quantity, discloseqty=0,
                                                  price_type=price_type, price=price, retention='DAY',
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
                            raise Exception(f"REJECTED order for symbol={tc.trading_symbol}")
                        else:
                            logger.info(f"Successfully placed order: {res}")
                        return res
                logger.warning(f"Order {order_id} not found in order book")
        return None

    def order_update_listener(self, in_message):
        logger.info(f'Received broker update: {in_message}')
        order_id = in_message['norenordno']
        tsym = in_message['tsym']
        buy_or_sell = in_message['trantype']
        sleep(1)
        contracts: [TradeContract] = db_utils.get_orders([order_id], buy_or_sell, in_message['exch'])

        if not contracts:
            return

        contract: TradeContract = contracts[0]

        if in_message['status'] == 'COMPLETE' and contract.status != str(ContractStatus.CLOSE):
            settle_required = False
            fill_price = in_message['flprc']

            msg = f"[{buy_or_sell}] Order executed successfully for symbol {tsym}, fill price: [{fill_price}], qty: [{in_message['qty']}]"
            self.loop.create_task(bot_utils.send_alert(msg))

            if buy_or_sell == 'B':
                contract.buy_price = fill_price
                contract.buy_timestamp = in_message['fltm']
            else:
                contract.sell_price = fill_price
                contract.sell_timestamp = in_message['fltm']

            if contract.status == str(ContractStatus.OPEN):
                contract.status = str(ContractStatus.CLOSE)
            elif contract.status == str(ContractStatus.PENDING):
                contract.status = str(ContractStatus.OPEN)
                settle_required = True
            db_utils.delete_stock_record(order_id, in_message['exch'])
            self.loop.create_task(db_utils.insert_stock_record(contract, in_message['exch']))

            if settle_required:
                self.loop.create_task(self.settlement_queue.put(order_id))

        elif in_message['status'] in ['REJECTED', 'INVALID_STATUS_TYPE', 'CANCELED']:
            msg = f"[{buy_or_sell}] Order [{order_id}/{tsym}] failed: [{in_message['rejreason']}]"
            self.loop.create_task(bot_utils.send_alert(msg, logging.WARN))
            if contract.status == str(ContractStatus.PENDING):
                db_utils.delete_stock_record(order_id, in_message['exch'])
            if contract.status == str(ContractStatus.OPEN):
                self.loop.create_task(self.settlement_queue.put(order_id))

    async def settlement_processor(self, contract_id: str):
        status = ContractStatus.PENDING
        token = None
        contracts = db_utils.get_orders([contract_id], exch='NFO')
        if not contracts:
            return
        contract: TradeContract = contracts[0]
        if contract.status == ContractStatus.CLOSE:
            logger.info(f"Contract {contract.trading_symbol}/{contract.buy_id} already closed")
            return
        await self.settlement_processor_internal(contract)

    @poller(sleep_time=1)
    async def settlement_processor_internal(self, contract: TradeContract, token=None):
        close_leg = get_close_leg_type(contract.open_leg)
        if contract.open_leg == 'B':
            exit_set = self.immediate_long_exit_set
            select_id = contract.buy_id
        else:
            exit_set = self.immediate_short_exit_set
            select_id = contract.sell_id

        can_settle = False
        try:

            if select_id in exit_set:
                can_settle = True
            else:
                if not token:
                    token = f"{contract.exchange}|{self.get_token(contract.trading_symbol, contract.exchange)}"
                    self.broker_service.manage_subscription([token])
                    await asyncio.sleep(5)

                last_price = float(self.broker_service.REFDATA_MAP[token]['lp'])
                if contract.open_leg == 'B':
                    can_settle = self.can_settle_long_contract(contract, last_price)
            if can_settle:
                await self.execute_trade(contract, get_close_leg_type(contract.open_leg))
                exit_set.discard(select_id)
        except Exception as e:
            await bot_utils.send_alert(str(e), logging.ERROR)
            await self.settlement_queue.put(select_id)

    def can_settle_long_contract(self, contract: TradeContract, last_price: float):
        if last_price <= contract.sl:
            emoji = "🌠"
            if last_price < contract.buy_price:
                emoji = "🛑"
            bot_utils.send_alert(f"{emoji} Trailing SL was hit for [BUY Contract] {contract.trading_symbol} {emoji}")
            return True

        # trailing sl
        if last_price >= contract.tgt:
            contract.sl = max(round(contract.tgt * 0.90, 2), 1.01 * contract.cmp)
            contract.tgt = round(contract.tgt * 1.03, 2)
            msg = f"Updated params for [BUY Contract] {contract.trading_symbol}, SL: {contract.sl}, tgt:{contract.tgt}"
            logger.info(msg)
            bot_utils.send_alert(msg)
            return False

    async def construct_dry_run_order_response(self, tc: TradeCandidate | TradeContract, order_type, quantity):
        res = {'norenordno': int(datetime.datetime.now().timestamp()) + random.randint(100, 1000), 'qty': quantity,
               'tsym': tc.trading_symbol, 'trantype': order_type, 'status': 'COMPLETE',
               'fltm': str(datetime.datetime.now()), 'exch': tc.exchange}

        token = f"{tc.exchange}|{self.get_token(tc.trading_symbol, tc.exchange)}"
        if tc.open_leg == order_type:
            self.broker_service.manage_subscription([token])
            await asyncio.sleep(5)
        res['flprc'] = self.broker_service.REFDATA_MAP[token]['lp']
        return res

    async def handle_dry_run_order_update(self, response: dict):
        await asyncio.sleep(30)
        self.order_update_listener(response)
