import asyncio
import logging
import pathlib
from asyncio import AbstractEventLoop

import pyotp
import yaml
from telethon import TelegramClient, events, client
from telethon.tl.types import PeerChannel

from services.derivative_trade_service import DerivativeTradeService
from services.equity_trade_service import EquityTradeService
from services.shoonya_broker_service import ShoonyaBrokerService
from services.trade_service import get_first_number, compute_tgt, compute_sl, compute_long_derivative_exits
from utils import bot_utils, db_utils
from utils.bot_utils import initialize_bot
from utils.db_utils import initialize_db
from utils.helpers import poller
from utils.models import TradeCandidate, TradeChannels, ContractStatus

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] (%(threadName)-9s) - %(name)s -  %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


async def filter_messages(event):
    message = event.raw_text.lower()
    if event.from_id and not isinstance(event.from_id, PeerChannel) and event.from_id.user_id == 6037318029:
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
        tc_list: [TradeCandidate] = []
        if TradeChannels.equity99.name == sender.username:
            tc_list.append(await handle_equity99_recommendations(message.split("\n")))
        elif TradeChannels.AngelOneAdvisory.name == sender.username:
            message = message.lower()
            if 'buy' in message:
                tc = await handle_angel_recommendations(message)
                if isinstance(tc, TradeCandidate):
                    tc_list.append(tc)
                # 'exit', 'book profit'
            elif 'exit' in message or 'book profit' in message:
                await handle_angel_exits(message)

        elif ":" in message:
            tc_list.extend(await handle_custom_recommendations(message.split("\n")))

        await equity_trade_service.create_trade_candidate(tc_list)


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
            logger.info(broker_service.get_pnl())

        else:
            msg = "Unable to start broker session"
            await bot_utils.send_alert(msg, logging.WARN)


def order_update_listener(message):
    exchange = message['exch']
    if exchange == 'NFO':
        derivative_trade_service.order_update_listener(message)
    else:
        equity_trade_service.order_update_listener(message)


# TODO add handling for short orders
async def handle_angel_exits(message):
    message = message.split('\n')
    ticker = ''
    tick_splitter = ''
    pos = 0
    for msg in message:
        if '@' in msg:
            price_splitter = '@'
        else:
            price_splitter = 'at'

        if 'book profit' in msg:
            tick_splitter = 'in'
            pos = 1
        elif 'exit from' in msg:
            tick_splitter = 'exit from'
        elif 'exit' in msg:
            tick_splitter = 'exit'

        if tick_splitter:
            ticker = msg.split(tick_splitter)[pos].split(price_splitter)[0].strip()
            break

    if 'pe' in ticker or 'ce' in ticker:
        tsym, exch = await equity_trade_service.get_tsym_and_exch(ticker.upper(), 'NFO')
        contracts = db_utils.get_active_contracts_by_symbol(tsym, 'B', exch)
        if contracts:
            for ctx in contracts:
                if ctx.status == str(ContractStatus.OPEN):
                    derivative_trade_service.immediate_long_exit_set.add(ctx.buy_id)
                    await bot_utils.send_alert(f"Exit message received for {tsym}/{ctx.buy_id}")


async def handle_angel_recommendations(message: str):
    cmp, tgt, sl = -1, -1, -1
    exch, tsym = '', ''
    message = message.split('\n')
    for msg in message:
        if 'buy' in msg:
            msg_split = msg.split(' ')
            if ' ce ' in msg or ' pe ' in msg:
                tick = msg.split('buy')[-1].split('1 lot')[0].strip().upper()
                tsym, exch = await equity_trade_service.get_tsym_and_exch(tick, 'NFO')
            else:
                tsym, exch = await equity_trade_service.get_tsym_and_exch(msg_split[2])
            cmp = float(msg_split[-1].rstrip('.'))
        if 'sl' in msg:
            m = msg.split('sl')[-1]
            sl = get_first_number(m.split(' '))
        if 'tgt' in msg:
            m = msg.split('tgt')[-1]
            tgt = get_first_number(m.split(' '))
    logger.info(f"[{tsym}]:{exch} {cmp}, {tgt}, {sl}")

    if cmp and sl and tsym and exch and cmp != -1 and sl != -1:
        if exch != 'NFO':
            tgt = compute_tgt(cmp, tgt)
            sl = compute_sl(cmp, sl)
            new_tgt = cmp + 0.5 * (tgt - cmp)
            return TradeCandidate(tsym, exch, cmp, new_tgt, sl, 'B', TradeChannels.AngelOneAdvisory)
        else:
            tgt, sl = compute_long_derivative_exits(cmp, tgt, sl)
            tc = TradeCandidate(tsym, exch, cmp, tgt, sl, 'B', TradeChannels.AngelOneAdvisory)
            await derivative_trade_service.prepare_trade(tc)
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
            tsym, exch = await equity_trade_service.get_tsym_and_exch(msg_split[0])
            if not tsym and prev_index >= 0:
                tsym, exch = await equity_trade_service.get_tsym_and_exch(message[prev_index])
        if 'coming days view' in msg and msg.isalnum():
            tgt = get_first_number(msg.split(' '), tgt)
        if 'test level' in msg and tgt == -1:
            tgt = get_first_number(msg.split(' '))
        if 'sl' in msg and sl == -1:
            sl = get_first_number(msg.split('sl'))

    logger.info(f"[{tsym}]:{exch} {cmp}, {tgt}, {sl}")

    if cmp and sl and tsym and exch and cmp != -1 and sl != -1:
        new_tgt = cmp + 0.7 * (tgt - cmp)
        return TradeCandidate(tsym, exch, cmp, compute_tgt(cmp, new_tgt), compute_sl(cmp, sl), 'B',
                              TradeChannels.equity99)
    else:
        logger.info(f"cannot create trade candidate from message:[{message}]")
        return None


async def handle_custom_recommendations(in_msg) -> [TradeCandidate]:
    tc_list = []
    for msg in in_msg:
        msg = msg.split(',')
        if len(msg) > 1:
            tsym, exch = await equity_trade_service.get_tsym_and_exch(msg[0].split(':')[-1].strip())
            cmp = float(msg[1].split(':')[-1].strip())
            if cmp and tsym and exch:
                sl = round(0.95 * cmp, 2)
                tgt = round(1.1 * cmp, 2)
                logger.info(f"[{tsym}]:{exch} {cmp}, {tgt}, {sl}")
                tc_list.append(TradeCandidate(tsym, exch, cmp, compute_tgt(cmp, tgt), compute_sl(cmp, sl), 'B',
                                              TradeChannels.CustomChannel))
            else:
                logger.info(f"cannot create trade candidate from message:[{in_msg}]")
    return tc_list


async def polling_activity():
    loop.create_task(equity_trade_service.polling_activity())
    loop.create_task(derivative_trade_service.polling_activity())


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    initialize_db()

    with open(str(pathlib.Path(__file__).parent.resolve()) + '/resources/app-config.yml') as f:
        app_config = yaml.load(f, Loader=yaml.FullLoader)
        tele_config = app_config['telegram']
        trade_config = app_config['trade']
        derivative_config = app_config['derivative']
        trade_config['max_open_trades'] = sum([val['max_open_trades'] for (_, val) in trade_config.items()])
        derivative_config['max_open_trades'] = sum(
            [val['max_open_trades'] for (_, val) in derivative_config['conf'].items()])

    # message_selectors = ['Special Situation', 'conviction' 'PSU STOCK', 'btst']
    message_selectors = ['cmp', '@', 'buy', 'exit', 'book profit', 'closing price']  # 'test level'

    client = TelegramClient('anon',
                            tele_config["api_id"],
                            tele_config["api_hash"])

    with client:
        loop: AbstractEventLoop = client.loop
        broker_service = ShoonyaBrokerService()
        equity_trade_service = EquityTradeService(broker_service, loop, trade_config)
        derivative_trade_service = DerivativeTradeService(broker_service, loop, derivative_config)

        loop.create_task(initialize_bot(tele_config))
        loop.create_task(broker_login())
        loop.create_task(telegram_login())

        asyncio.gather(equity_trade_service.startup_activity(), derivative_trade_service.startup_activity())
        loop.create_task(polling_activity())
        client.run_until_disconnected()
