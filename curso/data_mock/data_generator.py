import os 
import asyncio
import json
import random
import logging
import aiomqtt
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MQTT_BROKER = os.getenv("AWS_PUBLIC_IP")
MQTT_PORT = 1883
MQTT_TOPIC = "curso/smartaqua/telemetry"

# Constelação Smart-Aqua com 6 satélites em estados variados
satellites_state = {
    "SAT-AQUA-01": {"status": "OPTIMAL", "alt": 600.0, "bat": 98.0, "temp": -40.0, "lat": 15.0, "lon": -45.0, "fuel": 150.0},
    "SAT-AQUA-02": {"status": "DEGRADING", "alt": 598.0, "bat": 60.0, "temp": -10.0, "lat": -10.0, "lon": 120.0, "fuel": 45.0},
    "SAT-AQUA-03": {"status": "CRITICAL", "alt": 570.0, "bat": 15.0, "temp": 85.0, "lat": 45.0, "lon": 10.0, "fuel": 5.0},
    "SAT-AQUA-04": {"status": "MAINTENANCE", "alt": 605.0, "bat": 50.0, "temp": -30.0, "lat": 0.0, "lon": 0.0, "fuel": 100.0},
    "SAT-AQUA-05": {"status": "OPTIMAL", "alt": 610.0, "bat": 100.0, "temp": -42.0, "lat": -30.0, "lon": -60.0, "fuel": 180.0},
    "SAT-AQUA-06": {"status": "OFFLINE", "alt": 0.0, "bat": 0.0, "temp": 0.0, "lat": 0.0, "lon": 0.0, "fuel": 0.0}, # Simula um satélite "morto"
}

def generate_payload(sat_id, state):
    if state["status"] == "OFFLINE":
        return None # Não transmite dados

    if state["status"] != "MAINTENANCE":
        state["lat"] = (state["lat"] + random.uniform(-0.5, 0.5)) % 90
        state["lon"] = (state["lon"] + random.uniform(-0.5, 0.5)) % 180

    # Comportamentos baseados na saúde
    if state["status"] in ["OPTIMAL", "MAINTENANCE"]:
        state["bat"] = min(100.0, state["bat"] + random.uniform(0.1, 0.5))
        state["temp"] = max(-99.0, min(199.0, state["temp"] + random.gauss(0, 0.2)))
        state["fuel"] = max(0.0, state["fuel"] - random.uniform(0.001, 0.01))
        error_code = 0
        status_flag = "OK"
        packet_loss = random.uniform(0.0, 0.5)
        
    elif state["status"] == "DEGRADING":
        state["bat"] = max(0.0, state["bat"] - random.uniform(0.1, 0.8))
        state["temp"] = max(-99.0, min(199.0, state["temp"] + random.gauss(0.5, 0.8)))
        state["fuel"] = max(0.0, state["fuel"] - random.uniform(0.01, 0.05))
        error_code = random.choice([0, 104, 201, 305])
        status_flag = random.choice(["OK", "WARNING"])
        packet_loss = random.uniform(2.0, 15.0)
        
    else: # CRITICAL
        state["alt"] = max(1.0, state["alt"] - random.uniform(0.2, 1.0))  # mínimo 1 km
        state["bat"] = max(0.0, state["bat"] - random.uniform(1.0, 2.5))
        state["temp"] = min(199.0, state["temp"] + random.gauss(1.5, 3.0))  # máximo 199°C
        state["fuel"] = max(0.0, state["fuel"] - random.uniform(0.1, 0.5))
        error_code = random.choice([404, 500, 503, 999])
        status_flag = "ERROR"
        packet_loss = random.uniform(20.0, 80.0)

    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "satellite_id": sat_id,
        "health_status": state["status"],
        "telemetry": {
            "orbital": {
                "latitude": round(state["lat"], 4),
                "longitude": round(state["lon"], 4),
                "altitude_km": round(state["alt"], 2),
                "velocity_kms": round(max(0.001, random.gauss(7.66, 0.05) if state["status"] != "CRITICAL" else random.gauss(7.50, 0.2)), 3)
            },
            "power": {
                "battery_level_pct": round(state["bat"], 1),
                "solar_voltage_v": round(max(0.0, random.gauss(32.0, 1.5)) if state["bat"] < 100 else 0.0, 2),
                "current_draw_a": round(random.uniform(2.5, 8.5), 2)
            },
            "thermal": {
                "core_temp_c": round(state["temp"], 1),
                "external_temp_c": round(random.uniform(-150.0, 120.0), 1),
                "cooling_active": bool(state["temp"] > -10.0)
            },
            "propulsion": {
                "fuel_remaining_kg": round(state["fuel"], 2),
                "thruster_status": status_flag
            },
            "communications": {
                "signal_strength_dbm": random.randint(-110, -50),
                "packet_loss_pct": round(packet_loss, 2),
                "uplink_active": bool(random.random() > 0.1)
            }
        },
        "payload_sensors": { # Sensores da missão (Recursos Hídricos)
            "soil_moisture_index": round(random.uniform(0.0, 1.0), 3) if status_flag != "ERROR" else None,
            "surface_water_area_sqkm": random.randint(1000, 50000) if status_flag == "OK" else 0,
            "algae_bloom_index": round(random.uniform(0.0, 100.0), 1),
            "sea_surface_salinity_psu": round(max(0.0, random.gauss(35.0, 2.0)), 2),
            "ice_thickness_m": round(random.uniform(0.0, 3.5), 2) if abs(state["lat"]) > 60 else None
        },
        "diagnostics": {
            "cpu_usage_pct": random.randint(10, 95) if state["status"] != "OPTIMAL" else random.randint(10, 30),
            "memory_usage_mb": random.randint(512, 4096),
            "last_error_code": error_code
        }
    }
    return payload

async def publish_telemetry():
    logging.info(f"Conectando ao broker {MQTT_BROKER}:{MQTT_PORT}...")
    try:
        async with aiomqtt.Client(hostname=MQTT_BROKER, port=MQTT_PORT) as client:
            logging.info("Conectado! Iniciando transmissão da constelação...")
            while True:
                for sat_id, state in satellites_state.items():
                    payload = generate_payload(sat_id, state)
                    if payload: # Pula satélites OFFLINE
                        topic = f"{MQTT_TOPIC}/{sat_id}"
                        await client.publish(topic, payload=json.dumps(payload), qos=1)
                        logging.info(f"[{sat_id}] Publicado - Bat: {payload['telemetry']['power']['battery_level_pct']}% | Status: {state['status']}")
                
                await asyncio.sleep(0.3) # Intervalo de 0.3 segundo entre as leituras da constelação
                
    except Exception as e:
        logging.error(f"Erro: {e}")

if __name__ == "__main__":
    asyncio.run(publish_telemetry())