import asyncio
import json
import logging
import os
import websockets
from confluent_kafka import Producer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Environment Configuration
SYMBOL = os.environ.get('SYMBOL', 'btcusdt')
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:29092')
KAFKA_TOPIC = "trades"

def delivery_report(err, msg):
    """
    Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush().
    """
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    # Per requirements, we only log errors here, not every success.

async def connect_and_produce():
    # Setup Kafka Producer
    producer_config = {
        'bootstrap.servers': KAFKA_BROKER,
    }
    producer = Producer(producer_config)
    
    ws_url = f"wss://stream.binance.com:9443/ws/{SYMBOL.lower()}@trade"

    while True:
        try:
            logger.info(f"Connecting to Binance WebSocket: {ws_url}")
            async with websockets.connect(ws_url) as websocket:
                logger.info(f"Connected to {ws_url}. Listening for trades...")
                
                async for message in websocket:
                    raw_data = json.loads(message)
                    
                    # Normalize the message
                    # Why we use "T" (trade time) instead of "E" (event time):
                    # "T" represents the exact matching engine timestamp of the trade.
                    # "E" is merely the time the event was emitted by the WebSocket stream.
                    # For downstream aggregations (like OHLCV generation) and accurate 
                    # time-series analysis, we MUST group by when the trade actually happened ("T").
                    normalized_data = {
                        "symbol": raw_data["s"],
                        "price": float(raw_data["p"]),
                        "quantity": float(raw_data["q"]),
                        "trade_id": int(raw_data["t"]),
                        "timestamp_ms": int(raw_data["T"]),
                        "is_buyer_maker": bool(raw_data["m"])
                    }
                    
                    # Publish to Kafka
                    # Using the symbol as the key ensures that all trades for a specific symbol 
                    # are appended to the same partition, preserving strict chronological ordering.
                    producer.produce(
                        KAFKA_TOPIC,
                        key=normalized_data["symbol"],
                        value=json.dumps(normalized_data),
                        on_delivery=delivery_report
                    )
                    
                    # Serve delivery callbacks for previous produce() calls
                    producer.poll(0)
                    
                    # Log the published message
                    logger.info(f"Published trade: symbol={normalized_data['symbol']} "
                                f"price={normalized_data['price']} trade_id={normalized_data['trade_id']}")

        except websockets.ConnectionClosed as e:
            logger.warning(f"WebSocket connection closed: {e}. Reconnecting in 5 seconds...")
        except Exception as e:
            logger.error(f"Unexpected error: {e}. Reconnecting in 5 seconds...")
        
        # Sleep before reconnecting to avoid spamming the endpoint immediately on failure
        await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(connect_and_produce())
    except KeyboardInterrupt:
        logger.info("Producer shutting down...")
