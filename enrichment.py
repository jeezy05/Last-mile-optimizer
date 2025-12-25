from confluent_kafka import Consumer, Producer
import json
import requests
import time
import os
import sys
from datetime import datetime
import xml.etree.ElementTree as ET  # Add import
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Config (no keys needed for Open-Meteo)
TOMTOM_API_KEY = os.getenv('TOMTOM_API_KEY')  # Required for traffic
if not TOMTOM_API_KEY:
    raise ValueError("TOMTOM_API_KEY not found in environment. Please add it to .env file.")
API_TIMEOUT = 5

def read_config():
    config = {}
    with open("client.properties") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split("=", 1)
                config[parameter] = value.strip()
    return config

def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Delivery failed: {err}")
    else:
        print(f"[✓] Enriched to {msg.topic()} [{msg.partition()}]")

def wmo_to_main(code):
    """Map Open-Meteo WMO weather_code to main category"""
    if code == 0: return "Clear"
    if code in [1, 2, 3]: return "Clouds"
    if code in [45, 48]: return "Fog"
    if 51 <= code <= 67 or 80 <= code <= 99: return "Rain"
    if 71 <= code <= 77 or 85 <= code <= 86: return "Snow"
    return "Unknown"

def compute_severity_score(weather_data):
    """Severity from WMO code + precip/vis (0-1)"""
    if not weather_data: return 0.0
    score = 0.0
    code = weather_data["weather_condition_id"]
    # WMO tiers
    if 80 <= code <= 99: score = 0.6  # Heavy rain
    elif 51 <= code <= 67: score = 0.5  # Light-moderate rain
    elif 71 <= code <= 77 or 85 <= code <= 86: score = 0.6  # Snow
    elif code in [45, 48]: score = 0.4  # Fog
    elif code in [61,63,65]: score += 0.1  # Drizzle boost
    # Precip
    if weather_data["precip_mm_1h"] > 2.0: score += 0.2
    # Vis
    if weather_data["visibility_m"] < 1000: score += 0.3
    elif weather_data["visibility_m"] < 5000: score += 0.1
    return round(min(1.0, score), 2)


def compute_delivery_success_percentage(weather_data, traffic_data):
    """
    Calculate delivery success percentage (0-100%) based on:
    - Weather conditions (visibility, precipitation, wind)
    - Traffic conditions (congestion, delay)
    - Time-based factors
    
    Returns percentage (0-100)
    """
    success = 100.0  # Start at 100%
    
    # Weather impact (up to 40% reduction)
    weather_severity = compute_severity_score(weather_data)
    weather_impact = weather_severity * 40
    success -= weather_impact
    
    # Traffic impact (up to 35% reduction)
    delay_minutes = traffic_data["traffic_delay_minutes"]
    congestion_factor = 0.5 if traffic_data["is_congested"] else 0.0
    # Map delay to impact: 0 min = 0%, 30+ min = 35%
    delay_impact = min(35, (delay_minutes / 30) * 35 + (congestion_factor * 15))
    success -= delay_impact
    
    # Visibility impact (up to 15% reduction)
    visibility = weather_data.get("visibility_m", 5000)
    if visibility < 500:
        visibility_impact = 15
    elif visibility < 1000:
        visibility_impact = 10
    elif visibility < 5000:
        visibility_impact = 5
    else:
        visibility_impact = 0
    success -= visibility_impact
    
    # Precipitation impact (up to 10% reduction) - already counted in severity
    # Add small bonus for clear skies
    if weather_data["weather_condition_id"] == 0:
        success += 5
    
    # Ensure within bounds
    return round(max(0, min(100, success)), 2)

def get_weather(lat, lon, city):
    """Open-Meteo current—no key/fallback"""
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current=temperature_2m,relative_humidity_2m,precipitation,weather_code,windspeed_10m,visibility&timezone=auto"
    resp = requests.get(url, timeout=API_TIMEOUT)
    if resp.status_code != 200:
        raise ValueError(f"API fail: {resp.status_code}")
    data = resp.json()["current"]
    return {
        "weather_condition_id": data["weather_code"],
        "weather_main": wmo_to_main(data["weather_code"]),
        "weather_description": f"WMO {data['weather_code']}",
        "temp_celsius": data["temperature_2m"],
        "humidity_percent": data["relative_humidity_2m"],
        "wind_speed_ms": round(data["windspeed_10m"] / 3.6, 1),
        "visibility_m": data["visibility"],
        "precip_mm_1h": data["precipitation"],
        "cloud_percent": None
    }

def get_traffic(lat_from, lon_from):  # Single point
    url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?key={TOMTOM_API_KEY}&point={lat_from},{lon_from}"
    resp = requests.get(url, timeout=API_TIMEOUT)
    
    # Initialize defaults
    curr_speed = 0.0
    ff_speed = 50.0
    curr_tt = 0.0
    ff_tt = 0.0
    
    if resp.status_code != 200:
        print(f"[TomTom {resp.status_code}] → sim")
    else:
        try:
            data = resp.json()["flowSegmentData"]
            curr_speed = float(data.get("currentSpeed", 0))
            ff_speed = float(data.get("freeFlowSpeed", 50))
            curr_tt = float(data.get("currentTravelTime", 0))
            ff_tt = float(data.get("freeFlowTravelTime", 0))
        except (json.JSONDecodeError, KeyError):
            print("[JSON fail] → sim")
    
    delay_min = max(0, (curr_tt - ff_tt) / 60)
    return {
        "current_speed_kmh": curr_speed,
        "free_flow_speed_kmh": ff_speed,
        "current_travel_time_s": curr_tt,
        "free_flow_travel_time_s": ff_tt,
        "traffic_delay_minutes": delay_min,
        "is_congested": curr_speed < 0.7 * ff_speed
    }


def test_enrich(lat=52.37, lon=4.89, city="Amsterdam"):  # Sample from LaDe
    """Test APIs standalone"""
    print("Testing Open-Meteo...")
    weather = get_weather(lat, lon, city)
    print(json.dumps(weather, indent=2))
    severity = compute_severity_score(weather)
    print(f"Severity: {severity}")
    
    print("\nTesting TomTom...")
    traffic = get_traffic(lat, lon)  # Dummy to
    print(json.dumps(traffic, indent=2))

# Kafka Main (unchanged)
def main():
    config = read_config()
    input_topic = "Kafka_shopify"  # Match client.py
    output_topic = "delivery-task-enriched"
    
    producer = Producer(config)
    cconfig = config.copy()
    cconfig.update({
        'group.id': 'enrich-group-v1',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    })
    consumer = Consumer(cconfig)
    consumer.subscribe([input_topic])
    
    count, errors = 0, 0
    no_msg_count = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                no_msg_count += 1
                if no_msg_count % 10 == 0:
                    print(f"Waiting for events... (polled {no_msg_count})") 
                continue
            if msg.error():
                print(f"[CONSUMER ERROR] {msg.error()}")
                continue
            try:
                evt = json.loads(msg.value().decode('utf-8'))
                key = str(evt.get("order_id", "unknown"))
                
                # Use current waypoint location if available, else fallback to delivery point
                lat = evt.get("current_gps_lat", evt.get("delivery_gps_lat", 28.7041))
                lon = evt.get("current_gps_lng", evt.get("delivery_gps_lng", 77.1025))
                lat_from = evt.get("accept_gps_lat", lat)
                lon_from = evt.get("accept_gps_lng", lon)
                city = evt.get("city", "Unknown")
                
                # Extract waypoint info if available
                waypoint_info = ""
                if "waypoint_index" in evt:
                    waypoint_info = f" [WP {evt['waypoint_index']}/{evt.get('total_waypoints', '?')} - {evt.get('journey_progress', 0):.0%}]"
                
                weather = get_weather(lat, lon, city)
                traffic = get_traffic(lat_from, lon_from)
                
                severity = compute_severity_score(weather)
                is_rainy = weather["precip_mm_1h"] > 0
                is_low_vis = weather["visibility_m"] < 1000
                
                # Calculate expected delivery success percentage
                delivery_success_pct = compute_delivery_success_percentage(weather, traffic)
                
                enriched = {
                    **evt,
                    "enrichment": {
                        "enriched_at": datetime.now().astimezone().replace(tzinfo=None).isoformat() + "Z",
                        "query_lat": lat,
                        "query_lng": lon,
                        "weather": weather,
                        "traffic": traffic,
                        "expected_delivery_success_percentage": delivery_success_pct,
                        "risk_flags": {
                            "weather_severity_score": severity,
                            "traffic_delay_minutes": traffic["traffic_delay_minutes"],
                            "is_rainy": is_rainy,
                            "is_low_visibility": is_low_vis,
                            "is_congested": traffic["is_congested"]
                        }
                    }
                }
                
                producer.produce(output_topic, key=key, value=json.dumps(enriched), callback=delivery_report)
                producer.poll(0)
                count += 1
                print(f"[#{count}] {city}{waypoint_info}: Success={delivery_success_pct:.1f}% | Sev={severity} | Delay={traffic['traffic_delay_minutes']:.1f}m | Congested={traffic['is_congested']}")

                
            except KeyError as e:
                print(f"[SKIP] Missing field {e} in evt")
            except Exception as e:
                print(f"[SKIP evt {evt.get('order_id', 'unk')}] {str(e)[:50]}")
                errors += 1
    except KeyboardInterrupt:
        print(f"\n=== STATS: {count} processed | {errors} errors ===")
    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    #test_enrich()  # Uncomment to test APIs
    main()
