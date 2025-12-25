from confluent_kafka import Producer
import json
import requests
import time
from datetime import datetime, timedelta
import uuid
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ============================================================================
# OPENROUTESERVICE CONFIG
# ============================================================================

# Get your API key from https://openrouteservice.org/dev/#/signup
ORS_API_KEY = os.getenv('ORS_API_KEY')  # Demo key with free tier
if not ORS_API_KEY:
    raise ValueError("ORS_API_KEY not found in environment. Please add it to .env file.")
ORS_BASE_URL = "https://api.openrouteservice.org/v2/directions/driving-car/geojson"

# ============================================================================
# DIVERSE DELIVERY LOCATIONS IN INDIA (lat, lng, city_name, diversity_reason)
# ============================================================================

DELIVERY_LOCATIONS = [
    # (pickup_lat, pickup_lng, delivery_lat, delivery_lng, city_name, description)
    (19.0760, 72.8777, 19.1136, 72.8697, "Mumbai", "Coastal metro - high traffic"),
    (28.6139, 77.2090, 28.4595, 77.0266, "Delhi", "NCR region - extreme weather swings"),
    (13.0827, 80.2707, 13.1939, 80.2700, "Chennai", "Coastal city - monsoon impact"),
    (23.1815, 79.9864, 23.2156, 79.9469, "Indore", "Central India - moderate congestion"),
    (22.5726, 88.3639, 22.5413, 88.3425, "Kolkata", "Eastern metro - humid climate"),
    (12.9716, 77.5946, 13.0827, 77.5946, "Bangalore", "Tech hub - smooth traffic"),
    (21.1458, 79.0882, 21.1613, 79.0844, "Nagpur", "Orange city - clear weather patterns"),
    (15.2993, 75.1393, 15.3050, 75.1288, "Hubli", "Karnataka - varied terrain"),
    (18.5204, 73.8567, 18.5239, 73.8539, "Pune", "Hill station approach - weather variable"),
    (10.8505, 76.2711, 10.8667, 76.2400, "Kochi", "Backwater region - heavy rain zones"),
]

# ============================================================================
# OPENROUTESERVICE ROUTE FETCHING
# ============================================================================

def get_route_coordinates(pickup_lat, pickup_lng, delivery_lat, delivery_lng):
    """
    Fetch route coordinates from OpenRouteService Directions API
    Returns list of (lat, lng) tuples along the route
    """
    headers = {
        "Authorization": ORS_API_KEY,
        "Content-Type": "application/json"
    }
    
    body = {
        "coordinates": [
            [pickup_lng, pickup_lat],
            [delivery_lng, delivery_lat]
        ],
        "format": "geojson"
    }
    
    try:
        response = requests.post(ORS_BASE_URL, json=body, headers=headers, timeout=10)
        
        if response.status_code != 200:
            print(f"[ORS ERROR {response.status_code}]")
            print(f"  Response: {response.text[:200]}")
            raise ValueError(f"ORS API failed with status {response.status_code}: {response.text[:500]}")
        
        data = response.json()
        
        # Extract coordinates from the route geometry
        if "features" in data and len(data["features"]) > 0:
            route_feature = data["features"][0]
            coordinates = route_feature["geometry"]["coordinates"]
            # ORS returns [lng, lat], convert to [(lat, lng)]
            return [(coord[1], coord[0]) for coord in coordinates]
        else:
            raise ValueError(f"ORS returned no route data. Response: {data}")
            
    except Exception as e:
        raise RuntimeError(f"[ORS CRITICAL ERROR] {str(e)}")



# ============================================================================
# DELIVERY EVENT SCHEMA
# ============================================================================

def create_delivery_schema(order_num, route_coords, city_name):
    """
    Create delivery order schema with real-time waypoint tracking
    
    Returns list of waypoint events (one per location along route)
    """
    order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
    courier_id = f"CRR-{order_num % 5:03d}"
    
    # Create a delivery that takes 30-60 minutes
    pickup_time = datetime.now()
    delivery_duration = timedelta(minutes=30 + (order_num * 3) % 30)
    delivery_time = pickup_time + delivery_duration
    
    waypoint_events = []
    num_waypoints = len(route_coords)
    
    for wp_idx, (wp_lat, wp_lng) in enumerate(route_coords):
        # Interpolate timestamp along the route
        progress = wp_idx / max(1, num_waypoints - 1) if num_waypoints > 1 else 1.0
        current_timestamp = pickup_time + delivery_duration * progress
        
        # Create waypoint event
        event = {
            "order_id": order_id,
            "courier_id": courier_id,
            "city": city_name,
            "region_id": order_num % 10,
            "aoi_id": order_num % 50,
            "aoi_type": "delivery_zone",
            
            # Origin point (warehouse/pickup)
            "accept_gps_lat": route_coords[0][0],
            "accept_gps_lng": route_coords[0][1],
            "accept_time": pickup_time.isoformat() + "Z",
            
            # Destination point
            "delivery_gps_lat": route_coords[-1][0],
            "delivery_gps_lng": route_coords[-1][1],
            "delivery_time": delivery_time.isoformat() + "Z",
            
            # Current waypoint (real-time location)
            "current_gps_lat": wp_lat,
            "current_gps_lng": wp_lng,
            "current_timestamp": current_timestamp.isoformat() + "Z",
            
            # Waypoint metadata
            "waypoint_index": wp_idx,
            "total_waypoints": num_waypoints,
            "journey_progress": round(progress, 2),
            "route_distance_m": num_waypoints * 100,  # Approx 100m per waypoint
            
            "ds": datetime.now().strftime("%Y-%m-%d"),
        }
        
        waypoint_events.append(event)
    
    return waypoint_events


# ============================================================================
# PRODUCER: Real-time Delivery Events
# ============================================================================

def produce_delivery_events(topic, config, num_deliveries=10):
    """
    Produce real-time delivery events with route waypoints
    Each delivery creates multiple events (one per waypoint along the route)
    """
    print(f"\n{'='*70}")
    print(f"PRODUCER: Real-time Delivery Simulation - OpenRouteService Routes")
    print(f"Creating {num_deliveries} diverse deliveries across India")
    print(f"{'='*70}\n")
    
    producer = Producer(config)
    total_events = 0
    
    for delivery_idx in range(num_deliveries):
        # Select a delivery location
        (pickup_lat, pickup_lng, delivery_lat, delivery_lng, city_name, description) = DELIVERY_LOCATIONS[delivery_idx % len(DELIVERY_LOCATIONS)]
        
        print(f"\n[Delivery {delivery_idx + 1}] {city_name} - {description}")
        print(f"  Route: ({pickup_lat:.4f}, {pickup_lng:.4f}) → ({delivery_lat:.4f}, {delivery_lng:.4f})")
        
        # Fetch real route from OpenRouteService
        route_coords = get_route_coordinates(pickup_lat, pickup_lng, delivery_lat, delivery_lng)
        print(f"  Route waypoints: {len(route_coords)}")
        
        # Create waypoint events for this delivery
        waypoint_events = create_delivery_schema(delivery_idx, route_coords, city_name)
        
        # Produce each waypoint event to Kafka
        for wp_event in waypoint_events:
            key = wp_event["order_id"]
            
            producer.poll(0)
            producer.produce(
                topic=topic,
                key=key,
                value=json.dumps(wp_event),
                callback=delivery_report,
            )
            
            total_events += 1
            
            # Simulate real-time streaming with small delay
            time.sleep(0.1)
    
    producer.flush()
    print(f"\n{'='*70}")
    print(f"✓ Finished producing {total_events} waypoint events from {num_deliveries} deliveries")
    print(f"{'='*70}\n")

# ============================================================================
# KAFKA UTILITIES
# ============================================================================

def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Delivery failed: {err}")
    else:
        print(f"[✓] Status pushed to {msg.topic()} [{msg.partition()}]")


# ============================================================================
# KAFKA UTILITIES
# ============================================================================

def read_config():
    config = {}
    with open("client.properties") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split("=", 1)
                config[parameter] = value.strip()
    return config



# ============================================================================
# MAIN
# ============================================================================

def main():
    config = read_config()
    raw_topic = "Kafka_shopify"
    
    print("\n[START] Delivery Event Producer with OpenRouteService")
    print(f"Config loaded: {list(config.keys())}\n")
    
    # Produce delivery events with real routes
    produce_delivery_events(raw_topic, config, num_deliveries=10)

if __name__ == "__main__":
    main()