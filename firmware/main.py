import network
import uasyncio as asyncio
import json
import machine
import time
import select
import config

# Initialize Watchdog Timer (8.3s max on Pico, we'll feed it regularly)
wdt = machine.WDT(timeout=8000)

try:
    import uwebsockets.client as websockets
except ImportError:
    websockets = None
    print("WARNING: uwebsockets missing. Please install it.")

# --- HARDWARE PIN SETUP ---
# SHT41 Temp/Humidity (I2C)
i2c = machine.I2C(0, sda=machine.Pin(4), scl=machine.Pin(5), freq=400000)

# Capacitive Water Level Sensor (Active Low/High depending on the specific module)
# Assuming a generic module that pulls LOW when wet
WATER_LEVEL_PIN = machine.Pin(14, machine.Pin.IN, machine.Pin.PULL_UP)

# Relays / MOSFET logic pins
PUMP_PIN = machine.Pin(15, machine.Pin.OUT)
LIGHT_PIN = machine.Pin(16, machine.Pin.OUT)

# 40mm PWM Fan
FAN_PIN = machine.PWM(machine.Pin(17))
FAN_PIN.freq(1000)
FAN_PIN.duty_u16(0)

class BotanistNode:
    def __init__(self):
        self.wlan = network.WLAN(network.STA_IF)
        self.ws = None # In production, use `uwebsockets` or `asyncio.stream`
        self.pump_task = None
        self.fan_task = None
        self.last_water_ms = time.ticks_ms()
        
    async def connect_wifi(self):
        self.wlan.active(True)
        if not self.wlan.isconnected():
            print(f"Connecting to {config.WIFI_SSID}...")
            self.wlan.connect(config.WIFI_SSID, config.WIFI_PASS)
            
            # Wait for connection with timeout
            attempts = 0
            while not self.wlan.isconnected() and attempts < 15:
                wdt.feed() # Keep WDT happy while connecting
                await asyncio.sleep(1)
                attempts += 1
            
            if not self.wlan.isconnected():
                print("WiFi connection failed (timeout).")
                return False
                
        print("WiFi Connected. IP:", self.wlan.ifconfig()[0])
        return True

    async def connect_websocket(self):
        """
        Connects to the FastAPI WS server.
        Note: requires uwebsockets to be installed via mip.
        """
        if websockets is None:
            print("WebSocket client not available. Cannot connect.")
            return False
            
        token = getattr(config, 'WS_TOKEN', '')
        url_with_token = f"{config.WS_SERVER_URL}?token={token}"
        print(f"Connecting to {url_with_token}...")
        try:
            self.ws = websockets.connect(url_with_token)
            print("WebSocket Connected!")
            return True
        except Exception as e:
            print(f"WebSocket connection failed: {e}")
            self.ws = None
            return False

    def calc_crc8(self, data: bytes) -> int:
        """Calculates CRC-8 for SHT4x sensors"""
        crc = 0xFF
        for idx in range(2):
            crc ^= data[idx]
            for _ in range(8):
                if crc & 0x80:
                    crc = (crc << 1) ^ 0x31
                else:
                    crc <<= 1
        return crc & 0xFF

    async def read_sht41(self) -> tuple:
        """
        Reads Temperature and Humidity directly via I2C for the SHT41 sensor.
        0xFD is the high precision measurement command.
        """
        try:
            i2c.writeto(0x44, b'\xFD')
            await asyncio.sleep_ms(10) # Give sensor time to measure
            data = i2c.readfrom(0x44, 6)
            
            # Check CRC
            if self.calc_crc8(data[0:2]) != data[2] or self.calc_crc8(data[3:5]) != data[5]:
                print("SHT41 CRC Error!")
                return (None, None)
            
            temp_raw = (data[0] << 8) | data[1]
            hum_raw = (data[3] << 8) | data[4]
            
            temp_c = -45.0 + (175.0 * temp_raw / 65535.0)
            humidity = -6.0 + (125.0 * hum_raw / 65535.0)
            
            # Clamp humidity
            humidity = max(0.0, min(100.0, humidity))
            
            return round(temp_c, 2), round(humidity, 2)
        except Exception as e:
            print("SHT41 read error:", e)
            return (25.0, 50.0) # Safe fallback

    async def telemetry_loop(self):
        """Streams real-time environment data to the FastAPI server."""
        while True:
            temp, hum = await self.read_sht41()
            
            # Read capacitive sensor (assumes LOW = wet/water present)
            water_ok = (WATER_LEVEL_PIN.value() == 0)
            
            # Send payload if we got valid sensor readings
            if temp is not None and hum is not None:
                payload = {
                    "temp_c": temp,
                    "humidity": hum,
                    "water_level_ok": water_ok,
                }
                
                print("Sending Telemetry ->", payload)
                
                # If WS is connected, send payload
                if self.ws:
                    try:
                        self.ws.send(json.dumps(payload))
                    except Exception as e:
                        print(f"WS Send Error: {e}")
                        self.ws = None # Force reconnect
                
            await asyncio.sleep(60) # 60-second sampling rate

    async def command_loop(self):
        """Listens for tool calls from the AI Agent."""
        while True:
            if self.ws:
                try:
                    # Use select.poll to check if socket has data before calling blockingly
                    poller = select.poll()
                    poller.register(self.ws.sock, select.POLLIN)
                    
                    # poll with 0 timeout (non-blocking)
                    res = poller.poll(0)
                    if res:
                        cmd_str = self.ws.recv() 
                        if cmd_str:
                            cmd = json.loads(cmd_str)
                            self.handle_command(cmd)
                except Exception as e:
                    # Connection drops or timeout
                    print(f"WS Recv Error/Timeout: {e}")
                    self.ws = None
            
            await asyncio.sleep(1)
            
    def handle_command(self, cmd: dict):
        tool = cmd.get("tool")
        kwargs = cmd.get("kwargs", {})
        
        print(f"Executing: {tool} with args {kwargs}")
        
        if tool == "trigger_flood":
            self.last_water_ms = time.ticks_ms()
            dur_mins = kwargs.get("duration_minutes", 15)
            if self.pump_task:
                self.pump_task.cancel()
            self.pump_task = asyncio.create_task(self.run_pump(dur_mins))
            
        elif tool == "set_fan_speed":
            speed_pct = kwargs.get("percent", 0)
            dur_mins = kwargs.get("duration_minutes", 0)
            if self.fan_task:
                self.fan_task.cancel()
            self.fan_task = asyncio.create_task(self.run_fan(speed_pct, dur_mins))
            
        elif tool == "set_grow_light":
            state = kwargs.get("state", "OFF")
            LIGHT_PIN.value(1 if state == "ON" else 0)
            print("Light toggled:", state)

    async def run_pump(self, duration_mins: int):
        try:
            # Failsafe check: if WATER_LEVEL_PIN is HIGH (1), it means it's not wet (assuming LOW = wet)
            if WATER_LEVEL_PIN.value() != 0:
                print("[HW] FAILSAFE ENGAGED: PUMP BLOCKED - Water level low!")
                return
            
            # Hardcap pump run time
            safe_duration = min(duration_mins, 30)
            if safe_duration != duration_mins:
                print(f"[HW] WARNING: LLM requested {duration_mins}m. Capping to {safe_duration}m!")
            
            print(f"[HW] PUMP ON for {safe_duration} mins")
            PUMP_PIN.value(1)
            
            # Wait for duration (split into 1-sec sleeps to feed watchdog)
            for _ in range(safe_duration * 60):
                wdt.feed()
                # Check failsafe continuously
                if WATER_LEVEL_PIN.value() != 0:
                    print("[HW] FAILSAFE ENGAGED MID-CYCLE: PUMP BLOCKED - Water level low!")
                    break
                await asyncio.sleep(1)
                
        except asyncio.CancelledError:
            print("[HW] PUMP task cancelled (Duplicate command received)")
        finally:
            PUMP_PIN.value(0)
            print("[HW] PUMP OFF - Flood cycle complete")

    async def run_fan(self, speed_percent: int, duration_mins: int):
        try:
            print(f"[HW] FAN ON at {speed_percent}% for {duration_mins} mins")
            duty = int((speed_percent / 100.0) * 65535)
            FAN_PIN.duty_u16(duty)
            
            if duration_mins > 0:
                await asyncio.sleep(duration_mins * 60)
        except asyncio.CancelledError:
            print("[HW] FAN task cancelled (Duplicate command received)")
        finally:
            FAN_PIN.duty_u16(0)
            print("[HW] FAN OFF - Drying/Cooling complete")

async def offline_fallback_loop(node):
    """Fallback timer if server drops offline. Waters every 36h for 15m."""
    WATER_INTERVAL_MS = 36 * 60 * 60 * 1000 # 36 hours
    
    while True:
        if node.ws is None:
            now = time.ticks_ms()
            if time.ticks_diff(now, node.last_water_ms) > WATER_INTERVAL_MS:
                print("[FALLBACK] Server offline > 36h. Running emergency flood.")
                node.handle_command({"tool": "trigger_flood", "kwargs": {"duration_minutes": 15}})
                
        await asyncio.sleep(60)

async def main():
    node = BotanistNode()
    
    # Launch parallel generic tasks
    asyncio.create_task(node.telemetry_loop())
    asyncio.create_task(node.command_loop())
    asyncio.create_task(offline_fallback_loop(node))
    
    # Keep the main loop alive and manage connection state
    while True:
        wdt.feed()  # Feed watchdog
        if not node.wlan.isconnected():
            print("WiFi not connected. Reconnecting...")
            await node.connect_wifi()
            
        if node.wlan.isconnected() and node.ws is None:
            await node.connect_websocket()

        await asyncio.sleep(1)

if __name__ == "__main__":
    try:
        print("Starting Botanist Edge Node...")
        asyncio.run(main())
    except KeyboardInterrupt:
        # Safe shutdown state
        PUMP_PIN.value(0)
        LIGHT_PIN.value(0)
        FAN_PIN.duty_u16(0)
        print("\nShutdown complete. All relays disabled.")
