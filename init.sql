DROP TABLE IF EXISTS transactions;

CREATE TABLE transactions (
    tx_hash TEXT PRIMARY KEY,
    from_address TEXT,
    to_address TEXT,
    value DOUBLE PRECISION,
    gas INTEGER,
    timestamp BIGINT,
    is_token_tx BOOLEAN
);