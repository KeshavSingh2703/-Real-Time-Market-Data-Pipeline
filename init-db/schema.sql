-- Enable TimescaleDB extension if not already enabled
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- TABLE 1: trades
CREATE TABLE IF NOT EXISTS trades (
    time TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,
    trade_id BIGINT NOT NULL,
    is_buyer_maker BOOLEAN NOT NULL
);

-- Convert trades to hypertable
-- Why chunk_time_interval of 1 hour: 
-- Trades happen at very high frequency. A smaller chunk interval (1 hour) ensures 
-- that the active chunk fits entirely in memory, keeping inserts and recent reads fast.
SELECT create_hypertable('trades', 'time', chunk_time_interval => INTERVAL '1 hour', if_not_exists => TRUE);

-- Add composite index on (symbol, time DESC)
-- Why time DESC instead of ASC:
-- Analytical queries and time-series selections usually look for the most recent data first (e.g., latest trades). 
-- A descending (DESC) index allows the planner to find the freshest records immediately.
CREATE INDEX IF NOT EXISTS idx_trades_symbol_time ON trades (symbol, time DESC);


-- TABLE 2: ohlcv_bars
CREATE TABLE IF NOT EXISTS ohlcv_bars (
    time TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL,
    vwap DOUBLE PRECISION NOT NULL,
    trade_count INTEGER NOT NULL,
    
    -- What the UNIQUE constraint protects against:
    -- In a real-time message stream (like Kafka), at-least-once delivery could replay events.
    -- The UNIQUE constraint prevents duplicate bars for the same asset, timeframe, and start time,
    -- avoiding corrupted aggregates and allowing us to perform UPSERTs (ON CONFLICT).
    UNIQUE (symbol, interval, time)
);

-- Convert ohlcv_bars to hypertable
-- Why chunk_time_interval of 1 day:
-- Aggregated bars take up vastly less space than raw tick data. A larger chunk size (1 day) is used 
-- to reduce query planning overhead and the total number of chunks, since memory pressure is lower.
SELECT create_hypertable('ohlcv_bars', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- Add composite index on (symbol, interval, time DESC)
-- Similar to trades, time DESC optimizes for plotting or retrieving the most recent candles first.
CREATE INDEX IF NOT EXISTS idx_ohlcv_bars_symbol_interval_time ON ohlcv_bars (symbol, interval, time DESC);
