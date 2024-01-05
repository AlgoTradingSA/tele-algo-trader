import logging
import os
import sqlite3
from sqlite3 import Connection
from threading import Lock

from utils.models import TradeCandidate, TradeContract, ContractStatus, TradeChannels

logger = logging.getLogger(__name__)

conn: Connection = None

db_lock = Lock()

EXCH_TABLE_MAPPING = {'NSE': 'STOCK_RECORD', 'BSE': 'STOCK_RECORD', 'NFO': 'DERIVATIVE_RECORD'}

SR_SELECT_SQL = '''SELECT NAME, EXCHANGE, CMP, TGT, SL,
                   OPEN_LEG_TYPE,TRADE_CHANNEL,STATUS, QUANTITY, BUY_ORDER_ID, SELL_ORDER_ID, 
                   GTT_AL_ID, BUY_PRICE, SELL_PRICE, BUY_TIMESTAMP,SELL_TIMESTAMP, CREATED_TIMESTAMP '''


def initialize_db():
    global conn
    cwd = os.getcwd().replace('\\', '/')
    db_path = f'{cwd}/database.db'
    if not os.path.isfile(db_path):
        open(db_path, 'a').close()
    conn = sqlite3.connect(db_path, check_same_thread=False)

    with open(f'{cwd}/resources/create-tables.sql') as f:
        sql = f.read()
        conn.cursor().executescript(sql)


async def insert_stock_record(tc: TradeContract, exch: str = 'NSE'):
    table = EXCH_TABLE_MAPPING[exch]
    global conn
    with db_lock:
        conn.cursor().execute(
            f"INSERT INTO {table} (NAME, EXCHANGE, CMP, TGT, SL, QUANTITY, BUY_PRICE, SELL_PRICE, "
            "OPEN_LEG_TYPE, TRADE_CHANNEL, GTT_AL_ID,"
            "BUY_ORDER_ID, SELL_ORDER_ID,STATUS,BUY_TIMESTAMP,SELL_TIMESTAMP,  CREATED_TIMESTAMP)"
            "VALUES (:trading_symbol, :exchange, :cmp, :tgt, :sl, :qty, :buy_price,:sell_price, "
            ":open_leg, :trade_channel, :gtt_al_id, :buy_id, "
            ":sell_id, :status, :buy_timestamp, :sell_timestamp, :created_timestamp)", vars(tc))
        conn.commit()


def delete_stock_record(order_id: list | set | str | int, exch: str = 'NSE'):
    table = EXCH_TABLE_MAPPING[exch]

    if isinstance(order_id, (list, set)):
        where_clause = f" IN ({','.join('?' * len(order_id))})"
    else:
        where_clause = f" = ?"
        order_id = [order_id]
    global conn
    sql = f"DELETE FROM {table} WHERE BUY_ORDER_ID {where_clause}"
    logger.info(sql)
    with db_lock:
        conn.cursor().execute(sql, list(order_id))
        conn.commit()


async def insert_trade_candidate(tc: TradeCandidate):
    global conn
    with db_lock:
        conn.cursor().execute(
            "INSERT INTO TRADE_CANDIDATES (NAME, EXCHANGE, CMP, TGT, SL, OPEN_LEG_TYPE, TRADE_CHANNEL,"
            "CREATED_TIMESTAMP)"
            "VALUES (:trading_symbol, :exchange, :cmp, :tgt, :sl, :open_leg, :trade_channel, "
            ":created_timestamp)",
            vars(tc))
        conn.commit()


def get_sr_prices(order_id):
    global conn
    with db_lock:
        return conn.cursor().execute("SELECT SL, TGT FROM STOCK_RECORD WHERE BUY_ORDER_ID = ?",
                                     [order_id]).fetchone()


def update_gtt_status(gtt_status, order_id, al_id, fill_price=None, buy_timestamp=None):
    global conn
    where_clause = ' WHERE '
    set_clause = ' SET '
    sql_items = []
    is_first = True
    if gtt_status:
        set_clause = set_clause + "STATUS = ?"
        sql_items.append(str(gtt_status))
        is_first = False
    if fill_price:
        if not is_first:
            set_clause = set_clause + ', '
        set_clause = set_clause + "BUY_PRICE = ?"
        sql_items.append(fill_price)
        is_first = False
    if buy_timestamp:
        if not is_first:
            set_clause = set_clause + ', '
        set_clause = set_clause + "BUY_TIMESTAMP = ?"
        sql_items.append(buy_timestamp)
        is_first = False
    if al_id:
        if not is_first:
            set_clause = set_clause + ', '
        set_clause = set_clause + "GTT_AL_ID = ?"
        sql_items.append(al_id)
        is_first = False

    if isinstance(order_id, (list, set)):
        where_clause = where_clause + f"BUY_ORDER_ID in ({','.join('?' * len(order_id))})"
        sql_items.extend(order_id)
    else:
        where_clause = where_clause + "BUY_ORDER_ID = ?"
        sql_items.append(order_id)

    sql = f"UPDATE STOCK_RECORD {set_clause} {where_clause}"
    logger.info(f"SQL: {sql}, {sql_items}")
    with db_lock:
        conn.cursor().execute(sql, sql_items)
        conn.commit()


def delete_trade_candidate(tsym):
    global conn
    with db_lock:
        conn.cursor().execute("DELETE FROM TRADE_CANDIDATES "
                              "WHERE NAME = ?", [tsym])
        conn.commit()


async def get_gtt_pending_orders():
    global conn
    with db_lock:
        rows = conn.cursor().execute(f"{SR_SELECT_SQL}"
                                     "FROM STOCK_RECORD "
                                     f"WHERE STATUS = '{str(ContractStatus.GTT_PENDING)}'").fetchall()

    contracts = [TradeContract(*row) for row in rows]
    return contracts


def get_open_orders(exch: str = 'NSE') -> [TradeContract]:
    table = EXCH_TABLE_MAPPING[exch]
    global conn
    with db_lock:
        rows = conn.cursor().execute(f"{SR_SELECT_SQL}"
                                     f"FROM {table} "
                                     f"WHERE STATUS = '{str(ContractStatus.OPEN)}'").fetchall()

    contracts = [TradeContract(*row) for row in rows]
    return contracts


def purge_trade_candidates():
    global conn
    with db_lock:
        rows = conn.cursor().execute("DELETE FROM TRADE_CANDIDATES "
                                     "WHERE "
                                     "datetime(CREATED_TIMESTAMP) <= datetime('now', '-20 hours')")
        conn.commit()
        logger.info(f"Purged Trading candidates : {rows.rowcount}")


def get_trade_candidates(names: [str] = None):
    global conn
    where_clause = ''
    if names:
        where_clause = "WHERE NAME IN (" + ','.join('?' * len(names)) + ")"
    else:
        names = []
    with db_lock:
        rows = conn.cursor().execute(
            "SELECT NAME, EXCHANGE, CMP, TGT, SL, OPEN_LEG_TYPE, TRADE_CHANNEL, CREATED_TIMESTAMP "
            f"FROM TRADE_CANDIDATES {where_clause}"
            f"ORDER BY CREATED_TIMESTAMP ASC", names).fetchall()
    tc = [TradeCandidate(*row) for row in rows]
    return tc


def purge_closed_orders():
    global conn
    with db_lock:
        rows = conn.cursor().execute("DELETE FROM STOCK_RECORD "
                                     f"WHERE STATUS = '{str(ContractStatus.CLOSE)}' AND "
                                     "datetime(SELL_TIMESTAMP) <= datetime('now', '-365 Days')")
        conn.commit()
        logger.info(f"Purged Close orders: {rows.rowcount}")


def get_pending_orders(exch: str = 'NSE') -> [TradeContract]:
    table = EXCH_TABLE_MAPPING[exch]
    global conn
    with db_lock:
        rows = conn.cursor().execute(f"{SR_SELECT_SQL}"
                                     f"FROM {table} "
                                     f"WHERE STATUS = '{str(ContractStatus.PENDING)}'").fetchall()

    contracts = [TradeContract(*row) for row in rows]
    return contracts


def get_active_trade_count(trade_channel: TradeChannels = None, exch: str = 'NSE') -> int:
    table = EXCH_TABLE_MAPPING[exch]
    and_clause = ''
    sql_values = [str(ContractStatus.CLOSE)]
    if trade_channel:
        and_clause = "AND TRADE_CHANNEL IN (?, ?)"
        sql_values.extend([trade_channel.name, str(trade_channel)])

    global conn
    sql = f"SELECT COUNT(*) FROM {table} WHERE STATUS != ? {and_clause}"
    with db_lock:
        rows = conn.cursor().execute(sql, sql_values).fetchone()
    return int(rows[0])


def get_orders(order_ids: [str], buy_or_sell: str = 'B', exch: str = 'NSE') -> [TradeContract]:
    table = EXCH_TABLE_MAPPING[exch]

    if buy_or_sell == 'B':
        id_field = 'BUY_ORDER_ID'
    else:
        id_field = 'SELL_ORDER_ID'

    global conn
    with db_lock:
        rows = conn.cursor().execute(f"{SR_SELECT_SQL}"
                                     f"FROM {table} "
                                     f"WHERE {id_field} IN "
                                     f"(" + ','.join('?' * len(order_ids)) + ")", order_ids).fetchall()

    contracts = [TradeContract(*row) for row in rows]
    return contracts


async def get_active_contracts_by_symbol(tsym: str, open_leg='B', exch: str = 'NSE') -> [TradeContract]:
    table = EXCH_TABLE_MAPPING[exch]

    global conn
    with db_lock:
        rows = conn.cursor().execute(f"{SR_SELECT_SQL}"
                                     f"FROM {table} "
                                     f"WHERE NAME = ? "
                                     f"AND OPEN_LEG_TYPE = ? "
                                     f"AND STATUS != ?", [tsym, open_leg, str(ContractStatus.CLOSE)]).fetchall()

    contracts = [TradeContract(*row) for row in rows]
    return contracts
