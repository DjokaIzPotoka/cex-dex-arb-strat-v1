CREATE TABLE IF NOT EXISTS 'trades' (
    symbol          SYMBOL,
    side            SYMBOL,
    price           DOUBLE,
    amount          DOUBLE,
    trade_id        LONG,
    receive_ts      LONG,
    timestamp       TIMESTAMP,
    is_buyer_maker  BOOLEAN
) timestamp(timestamp) PARTITION BY DAY WAL;