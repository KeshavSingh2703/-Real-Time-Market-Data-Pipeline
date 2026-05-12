import asyncio
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone

import asyncpg
from confluent_kafka import Consumer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Config
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:29092')
DB_URL = os.environ.get('DB_URL', 'postgresql://trader:secret@localhost:5432/marketdata')
GROUP_ID = os.environ.get('GROUP_ID', 'ohlcv-consumer')

INTERVALS = {"1m": 60, "5m": 300, "1h": 3600}

@dataclass
class BarState:
    symbol: str
    interval: str
    bar_open_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    trade_count: int
    sum_pv: float
    sum_v: float

    @property
    def vwap(self) -> float:
        if self.sum_v == 0:
            return 0.0
        return self.sum_pv / self.sum_v

def get_bar_open_time(timestamp_ms: int, interval_seconds: int) -> datetime:
    """Floors the timestamp to the interval boundary."""
    floored_ms = (timestamp_ms // (interval_seconds * 1000)) * (interval_seconds * 1000)
    return datetime.fromtimestamp(floored_ms / 1000.0, tz=timezone.utc)

def process_trade(trade: dict, active_bars: dict, completed_bars: list):
    """
    Update running bars based on the trade event timestamp.
    
    Why event-time (trade timestamp) not wall-clock time for bar finalization:
    Market data is highly time-sensitive. If delays occur in processing (e.g. streaming pauses or backwards catchup), 
    using wall-clock time would finalize bars at completely incorrect moments. 
    Event-time ensures deterministic and consistent bars matching real-life behavior.
    """
    symbol = trade["symbol"]
    price = trade["price"]
    quantity = trade["quantity"]
    timestamp_ms = trade["timestamp_ms"]

    for interval_str, interval_secs in INTERVALS.items():
        bar_time = get_bar_open_time(timestamp_ms, interval_secs)
        key = (symbol, interval_str)

        if key not in active_bars:
            # First trade for this interval window creates a new bar
            active_bars[key] = BarState(
                symbol=symbol,
                interval=interval_str,
                bar_open_time=bar_time,
                open=price,
                high=price,
                low=price,
                close=price,
                volume=quantity,
                trade_count=1,
                sum_pv=price * quantity,
                sum_v=quantity
            )
        else:
            current_bar = active_bars[key]
            
            # If trade has structurally passed into a new time window, current bar is completed
            if bar_time > current_bar.bar_open_time:
                completed_bars.append(current_bar)
                
                # Start fresh bar for the new window
                active_bars[key] = BarState(
                    symbol=symbol,
                    interval=interval_str,
                    bar_open_time=bar_time,
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    volume=quantity,
                    trade_count=1,
                    sum_pv=price * quantity,
                    sum_v=quantity
                )
            elif bar_time == current_bar.bar_open_time:
                # Update existing bar
                current_bar.high = max(current_bar.high, price)
                current_bar.low = min(current_bar.low, price)
                current_bar.close = price
                current_bar.volume += quantity
                current_bar.trade_count += 1
                current_bar.sum_pv += price * quantity
                current_bar.sum_v += quantity

async def write_bars_to_db(pool, completed_bars: list):
    """
    Why UPSERT instead of INSERT for ohlcv_bars:
    In distributed systems, streams can sometimes redeliver events (At-Least-Once Delivery). 
    An UPSERT cleanly guarantees idempotence: modifying rather than crashing or duplicating 
    the bar for the same interval constraint.
    """
    if not completed_bars:
        return

    upsert_query = '''
        INSERT INTO ohlcv_bars (time, symbol, interval, open, high, low, close, volume, vwap, trade_count)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT ON CONSTRAINT uq_ohlcv_bar
        DO UPDATE SET
            open=EXCLUDED.open, 
            high=EXCLUDED.high, 
            low=EXCLUDED.low,
            close=EXCLUDED.close, 
            volume=EXCLUDED.volume, 
            vwap=EXCLUDED.vwap,
            trade_count=EXCLUDED.trade_count
    '''
    
    params = [
        (
            b.bar_open_time, b.symbol, b.interval, 
            b.open, b.high, b.low, b.close, 
            b.volume, b.vwap, b.trade_count
        ) for b in completed_bars
    ]
    
    async with pool.acquire() as conn:
        await conn.executemany(upsert_query, params)

async def write_trades_to_db(pool, raw_trades: list):
    if not raw_trades:
        return
        
    insert_query = '''
        INSERT INTO trades (time, symbol, price, quantity, trade_id, is_buyer_maker)
        VALUES ($1, $2, $3, $4, $5, $6)
    '''
    
    params = [
        (
            datetime.fromtimestamp(t["timestamp_ms"] / 1000.0, tz=timezone.utc),
            t["symbol"],
            t["price"],
            t["quantity"],
            t["trade_id"],
            t["is_buyer_maker"]
        ) for t in raw_trades
    ]
    
    async with pool.acquire() as conn:
        await conn.executemany(insert_query, params)

async def run_consumer():
    active_bars = {}
    completed_bars = []
    
    logger.info("Initializing asyncpg connection pool...")
    pool = await asyncpg.create_pool(DB_URL)

    consumer_config = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        # Why manual offset commit after DB write (not auto-commit):
        # Auto-commit risks committing the offset to Kafka even if database insertion fails or service crashes. 
        # Committing manually guarantees offset only advances successfully *after* writes are persisted, strictly 
        # ensuring we don't skip unpersisted messages.
        'enable.auto.commit': False 
    }
    
    consumer = Consumer(consumer_config)
    consumer.subscribe(['trades'])
    
    logger.info("Subscribed to 'trades' topic. Listening for messages...")
    
    try:
        while True:
            # Why we batch 100 messages before writing instead of writing each trade:
            # Real-time ticking streams emit vast numbers of points per second. Doing an isolated
            # network call for each DB insert overwhelms databases. Small batching arrays amortize 
            # roundtrips, massively expanding throughput threshold logic limit capabilities while maintaining near-real-time ingestion latency.
            batch_limit = 100
            msg_count = 0
            
            raw_trades = []
            
            # Accumulate a batch of messages
            while msg_count < batch_limit:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    break
                if msg.error():
                    logger.error(f"Kafka error: {msg.error()}")
                    continue
                
                trade = json.loads(msg.value().decode('utf-8'))
                raw_trades.append(trade)
                process_trade(trade, active_bars, completed_bars)
                msg_count += 1
            
            if raw_trades:
                await write_trades_to_db(pool, raw_trades)
                logger.info(f"Batched DB insert: {len(raw_trades)} raw trades written")
                
                if completed_bars:
                    await write_bars_to_db(pool, completed_bars)
                    logger.info(f"Batched DB upsert: {len(completed_bars)} completed bars written")
                    completed_bars.clear() 
                
                # Commit manually after both successful DB writes
                consumer.commit(asynchronous=False)
                
    except KeyboardInterrupt:
        logger.info("Interrupt received, consumer shutting down gracefully...")
    except Exception as e:
        logger.error(f"Unexpected consumer error: {e}", exc_info=True)
    finally:
        consumer.close()
        await pool.close()
        logger.info("Resources cleaned up.")

if __name__ == "__main__":
    asyncio.run(run_consumer())
