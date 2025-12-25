from confluent_kafka import Consumer
import json
import folium
import os

KAFKA_TOPIC = "Kafka_shopify"

def read_config():
    config = {}
    with open("client.properties") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split("=", 1)
                config[parameter] = value.strip()
    return config


def create_base_map(lat, lng):
    return folium.Map(location=[lat, lng], zoom_start=13)


def main():
    config = read_config()
    config["group.id"] = "map-consumer"
    config["auto.offset.reset"] = "earliest"

    consumer = Consumer(config)
    consumer.subscribe([KAFKA_TOPIC])

    print("Map consumer started. Open map.html in your browser and refresh as points update.")

    points = []  # store visited coordinates to draw a path

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            event = json.loads(msg.value().decode("utf-8"))
            lat = event.get("accept_gps_lat") or event.get("delivery_gps_lat")
            lng = event.get("accept_gps_lng") or event.get("delivery_gps_lng")

            if lat is None or lng is None:
                continue

            points.append((lat, lng))

            # Create / update map
            m = create_base_map(lat, lng)

            # Draw trail
            folium.PolyLine(points, color="blue", weight=3, opacity=0.8).add_to(m)

            # Current courier position marker
            folium.Marker(
                location=[lat, lng],
                popup=f"Courier {event.get('courier_id')} | Package {event.get('package_id')}",
                icon=folium.Icon(color="red", icon="truck", prefix="fa"),
            ).add_to(m)

            # Save to HTML (overwrite each time)
            m.save("map.html")
            print(f"Updated map at {lat}, {lng}")

    except KeyboardInterrupt:
        print("Stopping map consumer.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
