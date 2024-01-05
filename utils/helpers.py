import asyncio
import datetime
import logging
from zoneinfo import ZoneInfo

import pandas as pd
from cacheout import Cache
from pandas import DataFrame

from utils import bot_utils

IST_ZONE = 'Asia/Calcutta'
MARKET_OPEN_TIME = datetime.time(9, 15, 00, 00, ZoneInfo(IST_ZONE))
MARKET_CLOSE_TIME = datetime.time(15, 30, 00, 00, ZoneInfo(IST_ZONE))

nse_cache = Cache()


def poller(sleep_time=30):
    def poller_execution(func):
        async def execution(*args, **kwargs):
            while True:
                if (get_current_time() < MARKET_OPEN_TIME) or (get_current_time() > MARKET_CLOSE_TIME):
                    await asyncio.sleep(60)
                    continue
                else:
                    await func(*args, **kwargs)
                    await asyncio.sleep(sleep_time)

        return execution

    return poller_execution


def get_current_time():
    return datetime.datetime.now(ZoneInfo(IST_ZONE)).timetz()

@nse_cache.memoize()
def get_symbols() -> DataFrame:
    return pd.read_csv('https://api.shoonya.com/NFO_symbols.txt.zip').sort_values(by=['Symbol'], ascending=True)


@nse_cache.memoize()
def get_lot_size(symbol: str) -> int:
    df = get_symbols()
    try:
        return df.loc[df['TradingSymbol'] == symbol]['LotSize'].iloc[0]
    except IndexError as e:
        msg = f"Error getting Lot size for symbol {symbol}"
        bot_utils.send_alert(msg, logging.ERROR)
        return 0
