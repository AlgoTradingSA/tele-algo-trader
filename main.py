import asyncio
import datetime
import logging
import pathlib
from math import floor
from threading import Lock
from time import sleep
from zoneinfo import ZoneInfo

import pyotp
import yaml
from retry import retry
from telethon import TelegramClient, events, client

from services.shoonya_broker_service import ShoonyaBrokerService
from utils import db_utils, bot_utils
from utils.bot_utils import initialize_bot
from utils.db_utils import initialize_db
from utils.models import TradeCandidate, TradeContract, ContractStatus, SetQueue, TradeChannels

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] (%(threadName)-9s) - %(name)s -  %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def poller(func):
    async def execution(*args, **kwargs):
        while True:
            if (datetime.datetime.now(ZoneInfo(IST_ZONE)).timetz() < MARKET_OPEN_TIME) or \
                    (datetime.datetime.now(ZoneInfo(IST_ZONE)).timetz() > MARKET_CLOSE_TIME):
                await asyncio.sleep(60)
                continue
            else:
                await func(*args, **kwargs)

    return execution


async def filter_messages(event):
    message = event.raw_text.lower()
    if event.from_id and event.from_id.user_id == 6037318029:
        return False
    for word in message_selectors:
        if word in message:
            return True
    return False


async def telegram_login():
    await client.connect()
    if not await client.is_user_authorized():
        await client.send_code_request(tele_config.phone_number)
        me = await client.sign_in(tele_config.phone_number, input('Enter code: '))

    # channel_entity = await client.get_entity(channel_username)
    # print(channel_entity.username)

    @client.on(events.NewMessage(func=filter_messages, incoming=True))
    async def my_event_handler(event):
        message = event.message.message.replace('\n\n', '\n')
        logger.info(f'Received message: {message}')

        sender = await event.get_sender()
        tc: TradeCandidate = None
        if TradeChannels.equity99.name == sender.username:
            tc = await handle_equity99_recommendations(message.split("\n"))
        elif TradeChannels.AngelOneAdvisory.name == sender.username:
            message = message.lower()
            if ' ce ' not in message and ' pe ' not in message:
                if 'buy' in message:
                    tc = await handle_angel_recommendations(message)
                    # 'exit', 'book profit'
                elif 'exit' in message or 'book profit' in message:
                    await handle_angel_exits(message)

        async with lock:
            if tc and not (tc.trading_symbol in potential_trades_dict or tc.trading_symbol in open_trades):
                potential_trades_dict[tc.trading_symbol] = None
                await candidate_queue.put(tc)
                await db_utils.insert_trade_candidate(tc)


async def broker_login():
    with open(str(pathlib.Path(__file__).parent.resolve()) + '/resources/broker-cred.yml') as f:
        cred = yaml.load(f, Loader=yaml.FullLoader)
        logger.info("Logging into Broker API")
        ret = broker_service.login(userid=cred['user'], password=cred['pwd'], twoFA=pyotp.TOTP(cred['key']).now(),
                                   vendor_code=cred['vc'], api_secret=cred['apikey'], imei=cred['imei'])
        if ret:
            logger.info(f"Broker login successful, Session id: {ret['susertoken']}")
            broker_service.connect_websocket(order_update_listener)
            await bot_utils.send_alert("Successfully logged into broker account")

        else:
            msg = "Unable to start broker session"
            await bot_utils.send_alert(msg, logging.WARN)


def order_update_listener(in_message):
    logger.info(f'Received broker update: {in_message}')
    order_id = in_message['norenordno']
    tsym = in_message['tsym']
    sleep(1)
    # with thread_lock:
    if order_id and order_id in potential_trades_dict.values():  # check only if buy order was placed
        tran_type = in_message['trantype']
        if in_message['status'] == 'COMPLETE':
            fill_price = in_message['flprc']

            msg = f"[{tran_type}] Order executed successfully for symbol {tsym}, fill price: [{fill_price}], qty: [{in_message['qty']}]"
            loop.create_task(bot_utils.send_alert(msg))

            loop.create_task(gtt_order_handling_async(order_id, in_message['exch'], tsym, in_message['qty'], fill_price,
                                                      in_message['fltm']))

        elif in_message['status'] in ['REJECTED', 'INVALID_STATUS_TYPE', 'CANCELED']:
            potential_trades_dict[tsym] = None
            db_utils.delete_stock_record(order_id)
            msg = f"[{tran_type}] Order [{order_id}/{tsym}] failed: [{in_message['rejreason']}]"
            loop.create_task(bot_utils.send_alert(msg, logging.WARN))


async def gtt_order_handling_async(order_id, exchange, tsym, qty, fill_prc=None, fill_time=None):
    async with lock:
        gtt_order_handling(order_id, exchange, tsym, qty, fill_prc, fill_time)


def gtt_order_handling(order_id, exchange, tsym, qty, fill_prc=None, fill_time=None):
    sl, tgt = db_utils.get_sr_prices(order_id)
    is_ggt_placed, gtt_resp = place_cover_order('S', exchange, tsym, qty, tgt, sl)
    al_id = None
    if is_ggt_placed:
        msg = f'Successfully placed GTT order for [{tsym}/{order_id}]: {gtt_resp}'
        loop.create_task(bot_utils.send_alert(msg))
        status = ContractStatus.OPEN
        al_id = gtt_resp['al_id']
        open_trades.add(tsym)
        potential_trades_dict.pop(tsym, None)
    else:
        msg = f'Failed to place GTT order for [{tsym}/{order_id}]: {gtt_resp}'
        loop.create_task(bot_utils.send_alert(msg, logging.WARN))
        status = ContractStatus.GTT_PENDING
    db_utils.update_gtt_status(status, order_id, al_id, fill_prc, fill_time)
    db_utils.delete_trade_candidate(tsym)


def place_cover_order(buy_or_sell, exchange, trading_symbol, quantity, tgt, sl):
    resp = broker_service.place_oco_order(buy_or_sell, exchange, trading_symbol, quantity, tgt, sl)
    if resp and resp['stat'].lower() == 'oi created':
        return True, resp
    return False, resp


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


def compute_tgt(cmp: float, tgt: float):
    default_tgt = round(1.2 * cmp, 2)
    if not tgt:
        return default_tgt
    elif tgt > 1.5 * cmp:
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


async def get_tsym_and_exch(name: str) -> (str, str):
    if name:
        json = broker_service.searchscrip('NSE', name)
        if json:
            logger.debug(json['values'])
            return json['values'][0]['tsym'], json['values'][0]['exch']
    return None, None


async def handle_angel_exits(message):
    # TODO alert
    exit_selector = 'at'
    if '@' in message:
        exit_selector = '@'

    mlist = message.split(exit_selector)
    # TODO complete later


async def handle_angel_recommendations(message: str):
    cmp, tgt, sl = -1, -1, -1
    exch, tsym = '', ''
    message = message.split('\n')
    for msg in message:
        if 'buy' in msg:
            msg_split = msg.split(' ')
            tsym, exch = await get_tsym_and_exch(msg_split[2])
            cmp = float(msg_split[-1].rstrip('.'))
        if 'sl' in msg:
            m = msg.split('sl')[-1]
            sl = get_first_number(m.split(' '))
        if 'tgt' in msg:
            m = msg.split('tgt')[-1]
            tgt = get_first_number(m.split(' '))
    logger.info(f"[{tsym}]:{exch} {cmp}, {tgt}, {sl}")

    if cmp and sl and tsym and exch and cmp != -1 and sl != -1:
        tgt = compute_tgt(cmp, tgt)
        sl = compute_sl(cmp, sl)
        new_tgt = cmp + 0.5 * (tgt - cmp)
        return TradeCandidate(tsym, exch, cmp, new_tgt, sl, 'B', TradeChannels.AngelOneAdvisory)
    else:
        logger.info(f"cannot create trade candidate from message:[{message}]")
        return None


async def handle_equity99_recommendations(message: list[str]):
    cmp, tgt, sl = -1, -1, -1
    exch, tsym = '', ''
    for i, msg in enumerate(message):
        msg = msg.lower().replace('/', ' ')
        cmp_selector = ''
        prev_index = i - 1
        if 'cmp' in msg:
            cmp_selector = 'cmp'
        elif '@' in msg:
            cmp_selector = '@'
        if cmp_selector and cmp == -1:
            cmp = get_first_number(msg.split(' '))
            msg_split = msg.split(cmp_selector, maxsplit=1)
            tsym, exch = await get_tsym_and_exch(msg_split[0])
            if not tsym and prev_index >= 0:
                tsym, exch = await get_tsym_and_exch(message[prev_index])
        if 'coming days view' in msg and msg.isalnum():
            tgt = get_first_number(msg.split(' '), tgt)
        if 'test level' in msg and tgt == -1:
            tgt = get_first_number(msg.split(' '))
        if 'sl' in msg and sl == -1:
            sl = get_first_number(msg.split('sl'))

    logger.info(f"[{tsym}]:{exch} {cmp}, {tgt}, {sl}")

    if cmp and sl and tsym and exch and cmp != -1 and sl != -1:
        return TradeCandidate(tsym, exch, cmp, compute_tgt(cmp, tgt), compute_sl(cmp, sl), 'B', TradeChannels.equity99)
    else:
        logger.info(f"cannot create trade candidate from message:[{message}]")
        return None


@poller
async def trade_selector():
    if (len(set(potential_trades_dict.values()) - {None}) + len(open_trades)) >= trade_config['max_open_trades']:
        logger.info(f"Max trade limit of [{trade_config['max_open_trades']}] reached")
        await asyncio.sleep(300)
    elif not candidate_queue.empty():
        candidate = await candidate_queue.get()
        # logger.info(candidate)
        asyncio.create_task(execute_trade(candidate))
    await asyncio.sleep(30)


@retry(Exception, tries=3, delay=5)
async def execute_trade(tc: TradeCandidate):
    quantity = floor(trade_config['fund_alloc'] / tc.cmp)
    if quantity < 1:
        logger.warning(f"Unable to place trade for [{tc.trading_symbol}] with quantity: [{quantity}]")
        db_utils.delete_trade_candidate(tc.trading_symbol)
        potential_trades_dict.pop(tc.trading_symbol, None)
        return
    order_type = tc.open_leg
    msg = f"Placing [BUY] Order for symbol {tc.trading_symbol}, CMP: [{tc.cmp}], target: {tc.tgt}, " \
          f"quantity: {quantity}, stop_loss: {tc.sl}"
    await bot_utils.send_alert(msg)
    res = broker_service.place_order(buy_or_sell=order_type, product_type='C', exchange=tc.exchange,
                                     tradingsymbol=tc.trading_symbol, quantity=quantity, discloseqty=0,
                                     price_type='LMT', price=tc.cmp, retention='DAY',
                                     remarks=f'Buy order for {tc.trading_symbol}')

    if res is None:
        raise Exception(f"Failed to place order for symbol={tc.trading_symbol}")
    else:
        # print(res)
        await asyncio.sleep(0.1)
        order_id = res['norenordno']
        o_book = broker_service.get_order_book()

        for order in o_book:
            if order_id == order['norenordno']:
                if order['status'] == 'REJECTED':
                    msg = f"Order for [{tc.trading_symbol}] rejected by broker: {order['rejreason']}"
                    await bot_utils.send_alert(msg, logging.WARN)
                else:
                    async with lock:
                        potential_trades_dict[tc.trading_symbol] = order_id
                    contract = TradeContract.from_trade_candidate(tc, ContractStatus.PENDING, quantity,
                                                                  order_id)
                    await db_utils.insert_stock_record(contract)
                    logger.info(f"Successfully placed order: {res}")
                return
        logger.warning(f"Order {order_id} not found in order book")


# TESTING METHOD
async def gtt_testing(res, tc, order_id):
    logger.info("Testing here")
    res['status'] = 'COMPLETE'
    res['tsym'] = tc.trading_symbol
    res['trantype'] = 'B'
    res['rejreason'] = 'Testing'
    res['qty'] = 1
    res['flprc'] = tc.cmp
    res['exch'] = tc.exchange
    res['fltm'] = datetime.datetime.now()
    potential_trades_dict[tc.trading_symbol] = order_id
    contract = TradeContract.from_trade_candidate(tc, ContractStatus.PENDING, 1,
                                                  order_id)
    await db_utils.insert_stock_record(contract)
    order_update_listener(res)


def handle_pending_orders(startup: bool = False):
    pending_ids = set(potential_trades_dict.values()) - {None}
    if len(pending_ids) > 0:
        trade_book = broker_service.get_trade_book()
        if trade_book:
            for trade in trade_book:
                if trade['norenordno'] in pending_ids:
                    trade['status'] = 'COMPLETE'  # trade book always has completed trades
                    pending_ids.remove(trade['norenordno'])
                    order_update_listener(trade)
    if len(pending_ids) > 0:
        order_book = broker_service.get_order_book()
        if order_book:
            for order in order_book:
                if order['norenordno'] in pending_ids:
                    pending_ids.remove(order['norenordno'])
                    order_update_listener(order)
    if len(pending_ids) > 0 and startup:
        db_utils.delete_stock_record(pending_ids)


@poller
async def pending_poller():
    logger.info("Polling for Pending orders")
    async with lock:
        handle_pending_orders()
    await asyncio.sleep(600)


@poller
async def gtt_pending_poller():
    pending_contracts = await db_utils.get_gtt_pending_orders()
    logger.info(f"Polling for GTT pending orders, size=[{len(pending_contracts)}]")
    for contract in pending_contracts:
        await gtt_order_handling_async(contract.buy_id, contract.exchange, contract.trading_symbol, contract.qty)
    await asyncio.sleep(60)


@poller
async def candidate_poller():
    async with lock:
        candidates_tsym = list(key for (key, value) in reversed(potential_trades_dict.items()) if key and not value)
        logger.info(f"Polled Candidate list: {candidates_tsym}")
        candidates = db_utils.get_trade_candidates(candidates_tsym)
        logger.info(f"Polling for trade candidates, size=[{len(candidates)}]")
        for candidate in candidates:
            # logger.info(candidate)
            await candidate_queue.put(candidate)
    await asyncio.sleep(360)


def get_close_leg_type(open_leg_type: str) -> str:
    if open_leg_type == 'B':
        return 'S'
    else:
        return 'B'


@poller
async def open_trade_poller():
    contracts = await db_utils.get_open_orders()
    logger.info(f"Polling for OPEN orders, size=[{len(contracts)}]")

    trade_book = broker_service.get_trade_book()
    order_book = broker_service.get_order_book()
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
            contract.sell_price = trade['flprc']
            contract.sell_id = trade['norenordno']
            contract.sell_timestamp = trade['fltm']
            contract.status = str(ContractStatus.CLOSE)
            open_trades.remove(contract.trading_symbol)
            msg = f'Closed order successfully for [{contract.trading_symbol}/{contract.buy_id}]'
            db_utils.delete_stock_record(contract.buy_id)
            await db_utils.insert_stock_record(contract)
            await bot_utils.send_alert(msg)
            is_processed = True
        if not is_processed and (contract.trading_symbol, close_leg, contract.qty) in order_bk_dict:
            order = order_bk_dict[(contract.trading_symbol, close_leg, contract.qty)]
            if order['status'] in ['REJECTED', 'INVALID_STATUS_TYPE', 'CANCELED']:
                msg = f"[CLOSE Order [{order['tsym']}] failed: [{order['status']}] [{order['rejreason']}]"
                contract.status = str(ContractStatus.GTT_PENDING)
                db_utils.delete_stock_record(contract.buy_id)
                await db_utils.insert_stock_record(contract)
                await bot_utils.send_alert(msg, logging.WARN)
    await asyncio.sleep(240)


async def startup_activity():
    db_utils.purge_closed_orders()
    db_utils.purge_trade_candidates()
    tc = db_utils.get_trade_candidates()
    potential_trades_dict.update({t.trading_symbol: None for t in tc})
    pending_orders = db_utils.get_pending_orders()
    potential_trades_dict.update({p.trading_symbol: p.buy_id for p in pending_orders})
    logger.info(f"Initial Candidate list: {potential_trades_dict}")
    handle_pending_orders(True)

    open_trades.update([tc.trading_symbol for tc in db_utils.get_open_orders_internal()])
    logger.info(f"Open Trades: {open_trades}")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    initialize_db()
    candidate_queue = SetQueue()
    potential_trades_dict = {}  # {ticker-name: buy-order-id}
    open_trades = set()
    lock = asyncio.Lock()
    thread_lock = Lock()

    with open(str(pathlib.Path(__file__).parent.resolve()) + '/resources/app-config.yml') as f:
        app_config = yaml.load(f, Loader=yaml.FullLoader)
        tele_config = app_config['telegram']
        trade_config = app_config['trade']

    # message_selectors = ['Special Situation', 'conviction' 'PSU STOCK', 'btst']
    message_selectors = ['cmp', '@', 'buy', 'exit', 'book profit']  # 'test level'

    IST_ZONE = 'Asia/Calcutta'
    MARKET_OPEN_TIME = datetime.time(9, 15, 00, 00, ZoneInfo(IST_ZONE))
    MARKET_CLOSE_TIME = datetime.time(15, 30, 00, 00, ZoneInfo(IST_ZONE))

    client = TelegramClient('anon',
                            tele_config["api_id"],
                            tele_config["api_hash"])

    with client:
        loop = client.loop
        broker_service = ShoonyaBrokerService()

        loop.create_task(initialize_bot(tele_config))
        loop.create_task(broker_login())
        loop.create_task(telegram_login())

        asyncio.gather(startup_activity())

        loop.create_task(pending_poller())
        loop.create_task(gtt_pending_poller())
        loop.create_task(candidate_poller())
        loop.create_task(open_trade_poller())
        loop.create_task(trade_selector())
        client.run_until_disconnected()
