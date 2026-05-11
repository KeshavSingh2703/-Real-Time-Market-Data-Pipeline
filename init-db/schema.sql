-- Placeholder for TimescaleDB initialization
CREATE TABLE IF NOT EXISTS sample_data (
    time TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    price DOUBLE PRECISION NULL
);

-- Uncomment to turn into a hypertable
-- SELECT create_hypertable('sample_data', 'time');
