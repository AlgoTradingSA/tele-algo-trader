CREATE TABLE IF NOT EXISTS STOCK_RECORD (
NAME VARCHAR,
EXCHANGE VARCHAR,
QUANTITY INT,
CMP REAL,
TGT REAL,
SL REAL,
BUY_PRICE REAL,
SELL_PRICE REAL,
BUY_ORDER_ID INT,
SELL_ORDER_ID INT,
GTT_AL_ID INT,
STATUS VARCHAR,
OPEN_LEG_TYPE VARCHAR,
TRADE_CHANNEL VARCHAR,
BUY_TIMESTAMP TIMESTAMP,
SELL_TIMESTAMP TIMESTAMP,
CREATED_TIMESTAMP TIMESTAMP
);

CREATE TABLE IF NOT EXISTS TRADE_CANDIDATES (
NAME VARCHAR,
EXCHANGE VARCHAR,
CMP REAL,
TGT REAL,
SL REAL,
OPEN_LEG_TYPE VARCHAR,
TRADE_CHANNEL VARCHAR,
CREATED_TIMESTAMP TIMESTAMP
);


CREATE TABLE IF NOT EXISTS DERIVATIVE_RECORD (
NAME VARCHAR,
EXCHANGE VARCHAR,
QUANTITY INT,
CMP REAL,
TGT REAL,
SL REAL,
BUY_PRICE REAL,
SELL_PRICE REAL,
BUY_ORDER_ID INT,
SELL_ORDER_ID INT,
GTT_AL_ID INT,
STATUS VARCHAR,
OPEN_LEG_TYPE VARCHAR,
TRADE_CHANNEL VARCHAR,
BUY_TIMESTAMP TIMESTAMP,
SELL_TIMESTAMP TIMESTAMP,
CREATED_TIMESTAMP TIMESTAMP
);