import os
import asyncio
from datetime import datetime
from typing import Set, Dict, Any, List

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import asyncpg
from dotenv import load_dotenv

# Load environment variables (mostly for DB_URL)
load_dotenv()
DB_URL = os.getenv("DB_URL", "postgresql://trader:secret@localhost:5432/marketdata")

# ==============================================================================
# Connection Manager (Pub/Sub pattern)
#
# We use WebSockets for real-time market data to achieve push-based delivery
# over HTTP polling. Polling (e.g., dashboard asking for new prices every 1s)
# increases network and server load excessively, whereas WebSockets provide a
# persistent, low-latency, bidirectional connection ideal for trading streams.
#
# By using a module-level singleton of ConnectionManager, other modules in our
# pipeline or API (e.g., a background consumer task) can import `manager` or
# `broadcast_bar` and push the event out without being deeply coupled to
# FastAPI's internal application state. The Pub/Sub mechanism keeps a set of
# active client queues. When a new bar arrives, it is enqueued to all connected
# clients efficiently utilizing async queues.
# ==============================================================================
class ConnectionManager:
    def __init__(self):
        self.active_queues: Set[asyncio.Queue] = set()

    def connect(self, queue: asyncio.Queue):
        self.active_queues.add(queue)

    def disconnect(self, queue: asyncio.Queue):
        self.active_queues.discard(queue)

    async def broadcast(self, bar_dict: Dict[str, Any]):
        # Push the bar to all active WebSocket clients' queues
        for queue in self.active_queues.copy():
            try:
                queue.put_nowait(bar_dict)
            except asyncio.QueueFull:
                pass


manager = ConnectionManager()

# Exposed function to be imported by consumer script/process (if running in same instance)
async def broadcast_bar(bar_dict: Dict[str, Any]):
    await manager.broadcast(bar_dict)


# ==============================================================================
# Database Pool Lifespan Function
#
# In this architecture, we choose raw DB drivers (`asyncpg`) over heavy ORMs
# (like `SQLAlchemy`) for a few critical reasons for market data pipelines:
# 1. Performance: `asyncpg` is orders of magnitude faster when handling tens of
#    thousands of tick/bar inserts or bulk queries due to avoiding object
#    instantiation overhead.
# 2. Concurrency: Built natively for asyncio, meaning it will never block the
#    event loop, achieving massive concurrency for thousands of WebSocket connections.
# ==============================================================================
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic
    print("Initializing asyncpg connection pool...")
    app.state.pool = await asyncpg.create_pool(DB_URL)
    yield
    # Shutdown logic
    print("Closing asyncpg connection pool...")
    if app.state.pool:
        await app.state.pool.close()

# Initialize FastAPI application
app = FastAPI(title="Real-Time Market Data API", lifespan=lifespan)

# Setup cross origin resource sharing allowing all domains for ease of dev/dashboard integrations
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health_check():
    """Health check endpoint to ensure backend and server are operational."""
    return {"status": "ok"}


@app.get("/ohlcv")
async def get_ohlcv(
    symbol: str = Query(..., description="Target market symbol (e.g., BTCUSDT)"),
    interval: str = Query(..., description="Time interval (e.g., 1m, 5m, 1h)"),
    from_time: datetime = Query(..., alias="from", description="Datetime boundary inclusive lower bound"),
    to_time: datetime = Query(..., alias="to", description="Datetime boundary inclusive upper bound"),
    limit: int = Query(500, le=1000, description="Max amount of retrieved bars")
):
    """
    Retrieves OHLCV bars for a specified symbol and time window.
    """
    query = """
    SELECT time, symbol, interval, open, high, low, close, volume, vwap, trade_count
    FROM ohlcv_bars
    WHERE symbol = $1 AND interval = $2 AND time >= $3 AND time <= $4
    ORDER BY time ASC
    LIMIT $5
    """
    
    pool = app.state.pool
    async with pool.acquire() as connection:
        records = await connection.fetch(query, symbol, interval, from_time, to_time, limit)

    bars = [dict(record) for record in records]
    return bars


@app.get("/vwap")
async def get_vwap(
    symbol: str = Query(..., description="Target market symbol (e.g., BTCUSDT)"),
    window: str = Query(..., description="Time interval window (e.g., 1m, 5m, 1h)", pattern="^(1m|5m|1h)$")
):
    """
    Returns the most recent VWAP for the symbol at the given interval window.
    """
    query = """
    SELECT vwap, time, trade_count
    FROM ohlcv_bars
    WHERE symbol = $1 AND interval = $2
    ORDER BY time DESC
    LIMIT 1
    """
    
    pool = app.state.pool
    async with pool.acquire() as connection:
        record = await connection.fetchrow(query, symbol, window)

    if record:
        return dict(record)
    return {}


@app.websocket("/ws/live")
async def websocket_live_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint acting as subscriber for the live market data Pub/Sub pipeline.
    """
    await websocket.accept()
    
    # Give the client a dedicated queue using the standard size limit logic
    queue = asyncio.Queue()
    manager.connect(queue)
    
    try:
        # Keep consuming objects pushed into the client's queue and send over WS
        while True:
            # We await new bars and push to WebSocket
            bar_dict = await queue.get()
            
            # Format time properly before pushing as JSON
            if "time" in bar_dict and isinstance(bar_dict["time"], datetime):
                bar_dict["time"] = bar_dict["time"].isoformat()
                
            await websocket.send_json(bar_dict)
    except WebSocketDisconnect:
        # Client gracefully or aggressively disconnected
        manager.disconnect(queue)
    except Exception as e:
        print(f"WS error: {e}")
        manager.disconnect(queue)
