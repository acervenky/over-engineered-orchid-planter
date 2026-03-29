import json
import logging
import re
from datetime import datetime
import math
import pydantic
from typing import List, Optional, Literal, Dict, Any

from ollama import AsyncClient

logger = logging.getLogger("BotanistAgent")

class PumpKwargs(pydantic.BaseModel):
    duration_minutes: int

class FanKwargs(pydantic.BaseModel):
    percent: int
    duration_minutes: int

class LightKwargs(pydantic.BaseModel):
    state: Literal["ON", "OFF"]

class ToolCall(pydantic.BaseModel):
    tool: Literal["trigger_flood", "set_fan_speed", "set_grow_light"]
    kwargs: Dict[str, Any]

class DatabaseAction(pydantic.BaseModel):
    tool: str
    kwargs: dict

class DigitalBotanist:
    def __init__(self):
        # Using Qwen 9B as the underlying LLM via Ollama
        self.model_name = "qwen2.5:9b"  # Adjust tag (e.g., qwen1.5:9b or qwen2.5:14b) depending on your exact Ollama pull
        self.system_prompt = """You are the 'Digital Botanist', an autonomous AI agent managing a bare-root Tolumnia orchid in a micro-climate bioreactor.

Your strict biological constraints:
1. RESTORE WET/DRY CYCLES: Orchids need periods of being drenched, followed by complete drying computed via Vapor Pressure Deficit (VPD). Consider 'last_watered_time' in the telemetry before flooding.
2. TISSUE ABSORPTION MINIMUM: When triggering a flood, you MUST set a minimum 15-minute duration (e.g. `duration_minutes: 15`) so the velamen layer can absorb nutrients.
3. ABSOLUTELY NO NIGHTTIME WATERING: If the telemetry is_nighttime flag is true, you must wait until morning to flood the roots to prevent catastrophic rot. Never water at night.
4. VPD COOLING: If temperature is > 30°C and VPD is high, use the fan tool to provide cooling drafts, or adjust humidity by triggering micro-floods if needed.
5. ACTION HISTORY: You will receive 'recent_history' in the telemetry. Review your past actions to avoid oscillating commands. For example, if you recently commanded the fan to run for 60m, do not send conflicting fan commands until necessary.

You have access to the following tools:
- {"tool": "trigger_flood", "kwargs": {"duration_minutes": int}}
- {"tool": "set_fan_speed", "kwargs": {"percent": int, "duration_minutes": int}}
- {"tool": "set_grow_light", "kwargs": {"state": str}}  // "ON" or "OFF"

Evaluate the real-time telemetry array provided by the user. 
If an action is required, mathematically justify it and return a valid JSON array of tool calls.
Example return format: [{"tool": "trigger_flood", "kwargs": {"duration_minutes": 15}}]

CRITICAL: If an action is required, ONLY output the JSON array natively.
If no action is required, return an empty array [].
"""

    def calculate_vpd(self, temp_c: float, humidity_percent: float) -> float:
        # Calculate Saturation Vapor Pressure (SVP) in kPa
        svp = 0.61078 * math.exp((17.27 * temp_c) / (temp_c + 237.3))
        # Calculate Actual Vapor Pressure (AVP)
        avp = svp * (humidity_percent / 100.0)
        # VPD is the difference
        return round(svp - avp, 2)
        
    async def evaluate_environment(self, telemetry: dict) -> list:
        # Safely extract temp and humidity, default to normal room if missing
        temp_c = telemetry.get('temp_c', 25.0)
        humidity = telemetry.get('humidity', 50.0)
        
        vpd_kpa = self.calculate_vpd(temp_c, humidity)
        telemetry['calculated_vpd_kpa'] = vpd_kpa
        
        now = datetime.now()
        telemetry['current_local_time'] = now.strftime("%Y-%m-%d %H:%M:%S")
        telemetry['is_nighttime'] = (now.hour < 6 or now.hour >= 18)
        
        # If water level is critically low, block the pump tool conceptually
        if not telemetry.get('water_level_ok', True):
            logger.warning("Reservoir water level is low. Flooding risk blocked.")
            telemetry['SYSTEM_WARNING'] = "Pump is disabled due to low capacitance on water level sensor."

        prompt = f"Current Telemetry: {json.dumps(telemetry)}\nDetermine what actions, if any, are needed. Return ONLY the JSON array."
        
        try:
            client = AsyncClient()
            response = await client.chat(model=self.model_name, messages=[
                {"role": "system", "content": self.system_prompt},
                {"role": "user", "content": prompt}
            ])
            
            content = response['message']['content'].strip()
            
            # Non-greedy match for a JSON array
            match = re.search(r'\[.*?\]', content, re.DOTALL)
            if match:
                json_str = match.group(0)
                try:
                    actions_raw = json.loads(json_str)
                    if isinstance(actions_raw, list):
                        actions = []
                        for act in actions_raw:
                            try:
                                # Validate against Pydantic schema
                                validated = ToolCall(**act)
                                # Further validation on kwargs structure
                                if validated.tool == "trigger_flood":
                                    PumpKwargs(**validated.kwargs)
                                elif validated.tool == "set_fan_speed":
                                    FanKwargs(**validated.kwargs)
                                elif validated.tool == "set_grow_light":
                                    LightKwargs(**validated.kwargs)
                                actions.append(validated.model_dump())
                            except pydantic.ValidationError as e:
                                logger.error(f"Invalid tool call from LLM: {act}. Error: {e}")
                        
                        # Safety net: If LLM returned valid actions but skipped watering, ensure we don't starve the plant
                        has_flood = any(a.get("tool") == "trigger_flood" for a in actions)
                        if not has_flood:
                            safety_fallback = self.fallback_evaluation(telemetry)
                            if safety_fallback:
                                logger.warning("LLM omitted hydration, but 24h critical threshold reached. Enforcing fallback.")
                                actions.extend(safety_fallback)
                                
                        return actions
                except json.JSONDecodeError:
                    pass
            
            # If the LLM decided on no action, enforce our critical threshold safety net
            safety_fallback = self.fallback_evaluation(telemetry)
            if safety_fallback:
                logger.warning("LLM omitted hydration, but 24h critical threshold reached. Enforcing fallback.")
                return safety_fallback
            
            return []
            
        except Exception as e:
            logger.error(f"Error evaluating environment via Ollama: {e}")
            return self.fallback_evaluation(telemetry)
            
    def fallback_evaluation(self, telemetry: dict) -> list:
        """Algorithmic fallback if LLM is offline or refusing to hydrate past critical limits."""
        try:
            # 1. Biological strict blocks
            if telemetry.get('is_nighttime', False):
                return []
            if not telemetry.get('water_level_ok', True):
                return []
                
            # 2. Check time since last water
            last_watered = telemetry.get('last_watered_time', 'Never')
            if last_watered == 'Never':
                logger.warning("Fallback: Bootstrapping new plant with initial 15m flood.")
                return [{"tool": "trigger_flood", "kwargs": {"duration_minutes": 15}}]

            last_watered_dt = datetime.strptime(last_watered, "%Y-%m-%d %H:%M:%S")
            hours_since = (datetime.now() - last_watered_dt).total_seconds() / 3600
            
            # 3. Trigger at 24 hours
            if hours_since >= 24.0:
                logger.warning(f"Fallback: It has been {hours_since:.1f} hours since last water. Triggering 15m flood.")
                return [{"tool": "trigger_flood", "kwargs": {"duration_minutes": 15}}]
        except Exception as e:
            logger.error(f"Error in fallback evaluation: {e}")
            
        return []
