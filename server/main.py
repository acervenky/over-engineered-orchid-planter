import asyncio
import json
import logging
import time
import os
import aiosqlite
from typing import Dict, Any, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, status
from pydantic import BaseModel
from agent import DigitalBotanist

app = FastAPI(title="Smart Tolumnia Digital Botanist")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] BotanistServer: %(message)s")
logger = logging.getLogger("BotanistServer")

EXPECTED_TOKEN = os.getenv("BOTANIST_SECRET", "supersecret_botanist_token")

class Telemetry(BaseModel):
    temp_c: float
    humidity: float
    water_level_ok: bool
    # other fields might be added dynamically

# Connection Manager to prevent global mutable state issues
class ConnectionManager:
    def __init__(self):
        self.active_connection: WebSocket = None

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        # If an old connection exists, close it
        if self.active_connection:
            try:
                await self.active_connection.close()
            except Exception:
                pass
        self.active_connection = websocket

    def disconnect(self, websocket: WebSocket):
        if self.active_connection == websocket:
            self.active_connection = None

manager = ConnectionManager()
botanist = DigitalBotanist()

# Database setup
async def init_db():
    async with aiosqlite.connect("botanist_history.db") as db:
        await db.execute('''CREATE TABLE IF NOT EXISTS action_history 
                     (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                      timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                      telemetry TEXT,
                      actions TEXT)''')
        
        # Migrate table to add contains_flood column seamlessly
        try:
            await db.execute("ALTER TABLE action_history ADD COLUMN contains_flood BOOLEAN DEFAULT 0")
            # Update legacy rows to correctly set the flag
            await db.execute("UPDATE action_history SET contains_flood = 1 WHERE actions LIKE '%trigger_flood%'")
        except Exception:
            pass # Column already exists
            
        # Add index for instantaneous timestamp lookups
        try:
            await db.execute("CREATE INDEX IF NOT EXISTS idx_contains_flood ON action_history (contains_flood, timestamp DESC)")
        except Exception:
            pass
            
        await db.commit()

async def log_action(telemetry, actions):
    has_flood = any(a.get("tool") == "trigger_flood" for a in actions)
    async with aiosqlite.connect("botanist_history.db") as db:
        await db.execute("INSERT INTO action_history (telemetry, actions, contains_flood) VALUES (?, ?, ?)", 
                  (json.dumps(telemetry), json.dumps(actions), 1 if has_flood else 0))
        await db.commit()

async def get_last_watered_time() -> str:
    async with aiosqlite.connect("botanist_history.db") as db:
        # Utilizing the new indexed contains_flood boolean instead of FULL TABLE SCAN
        async with db.execute("SELECT timestamp FROM action_history WHERE contains_flood = 1 ORDER BY timestamp DESC LIMIT 1") as cursor:
            row = await cursor.fetchone()
            return row[0] if row else "Never"

async def get_recent_history(limit: int = 5) -> list:
    async with aiosqlite.connect("botanist_history.db") as db:
        async with db.execute("SELECT timestamp, actions FROM action_history ORDER BY timestamp DESC LIMIT ?", (limit,)) as cursor:
            rows = await cursor.fetchall()
            history = []
            for row in reversed(rows): # Provide chronological order
                history.append({"time": row[0], "actions": json.loads(row[1])})
            return history

async def prune_db_task():
    while True:
        try:
            async with aiosqlite.connect("botanist_history.db") as db:
                await db.execute("DELETE FROM action_history WHERE timestamp <= datetime('now', '-30 days')")
                await db.commit()
            logger.info("Database pruning completed.")
        except Exception as e:
            logger.error(f"Error pruning database: {e}")
        await asyncio.sleep(86400) # Sleep for 24 hours

# In-memory Server State
class ServerState:
    def __init__(self):
        self.last_eval_time = 0.0
        self.last_telemetry: Optional[dict] = None
        self.last_actions: Optional[list] = None

state = ServerState()
EVAL_INTERVAL_SECONDS = 900 # 15 minutes

@app.on_event("startup")
async def startup_event():
    await init_db()
    asyncio.create_task(prune_db_task())

@app.get("/status")
async def get_status():
    last_watered = await get_last_watered_time()
    return {
        "status": "ok",
        "last_telemetry": state.last_telemetry,
        "last_actions": state.last_actions,
        "last_watered_time": last_watered,
        "ws_connected": manager.active_connection is not None
    }

@app.websocket("/ws/telemetry")
async def telemetry_endpoint(websocket: WebSocket, token: Optional[str] = Query(None)):
    if token != EXPECTED_TOKEN:
        logger.warning(f"Rejected WS connection due to invalid token: {token}")
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    await manager.connect(websocket)
    logger.info("Pico W Connected. Receiving micro-climate telemetry...")
    
    try:
        while True:
            data = await websocket.receive_text()
            try:
                # Basic validation using pydantic (will raise if critical fields are missing/wrong type)
                telemetry_model = Telemetry.model_validate_json(data)
                telemetry = json.loads(data) # Use the raw dict for the agent
            except Exception as e:
                logger.error(f"Invalid telemetry payload: {data} -> {e}")
                continue

            state.last_telemetry = telemetry
            logger.info(f"Received telemetry: {telemetry}")
            
            current_time = time.time()
            if current_time - state.last_eval_time >= EVAL_INTERVAL_SECONDS:
                logger.info("Evaluating telemetry with LLM...")
                state.last_eval_time = current_time
                
                # Attach history so the AI has context
                telemetry['last_watered_time'] = await get_last_watered_time()
                telemetry['recent_history'] = await get_recent_history(5)
                
                # AI Agent evaluates the situation based on strict biological rules
                actions = await botanist.evaluate_environment(telemetry)
                
                if actions:
                    state.last_actions = actions
                    await log_action(telemetry, actions)
                    for action in actions:
                        logger.info(f"Dispatching action to Pico W: {action}")
                        if manager.active_connection:
                            await manager.active_connection.send_text(json.dumps(action))
                    
    except WebSocketDisconnect:
        logger.warning("Pico W Disconnected. Pausing automated operations.")
        manager.disconnect(websocket)

if __name__ == "__main__":
    import uvicorn
    # Make sure to run the server from the server directory or pass the correct module path
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
