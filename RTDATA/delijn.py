import asyncio
import aiohttp
import tempfile
import zipfile
import csv
from datetime import datetime, timezone
from google.transit import gtfs_realtime_pb2

# ================= CONFIG =================
API_KEY = "be478b0078c1b1c31c5a5d0254161a496f07bcacb26cc40aad0b5c226e51e1397705f5d79720e457739d9acbcdb2832062f4ae2cdd68e62d93880c7a9a2d5162"

GTFS_STATIC_URL = "https://api.mobilitytwin.brussels/de-lijn/gtfs"
GTFS_REALTIME_URL = "https://api.mobilitytwin.brussels/de-lijn/gtfs-realtime"

HEADERS = {
    "Authorization": f"Bearer {API_KEY}"
}

# ================= LOAD STATIC GTFS =================
def load_static_gtfs():
    import requests

    print("Downloading static GTFS...")
    data = requests.get(GTFS_STATIC_URL, headers=HEADERS).content

    with tempfile.NamedTemporaryFile(suffix=".zip") as f:
        f.write(data)
        f.flush()

        with zipfile.ZipFile(f.name, 'r') as z:

            def read_csv(name):
                with z.open(name) as file:
                    return list(csv.DictReader(line.decode('utf-8') for line in file))

            routes = read_csv("routes.txt")
            trips = read_csv("trips.txt")
            stops = read_csv("stops.txt")

    # Index maps
    routes_map = {r["route_id"]: r for r in routes}
    trips_map = {t["trip_id"]: t for t in trips}
    stops_map = {s["stop_id"]: s for s in stops}

    print("Static GTFS loaded.")
    return routes_map, trips_map, stops_map


# ================= MAIN STREAM =================
async def run():
    routes_map, trips_map, stops_map = load_static_gtfs()

    type_map = {
        "0": "Tram",
        "1": "Metro",
        "2": "Train",
        "3": "Bus"
    }

    async with aiohttp.ClientSession() as session:
        while True:
            async with session.get(GTFS_REALTIME_URL, headers=HEADERS) as resp:
                data = await resp.read()

                feed = gtfs_realtime_pb2.FeedMessage()
                feed.ParseFromString(data)

                print(f"\n--- Update {datetime.now(timezone.utc)} ---")

                for entity in feed.entity:

                    if entity.HasField("vehicle"):
                        v = entity.vehicle

                        trip_id = v.trip.trip_id
                        trip = trips_map.get(trip_id, {})
                        route_id = trip.get("route_id")

                        route = routes_map.get(route_id, {})

                        route_name = route.get("route_short_name", "N/A")
                        route_color = route.get("route_color", "N/A")
                        route_type = route.get("route_type", "N/A")

                        direction_id = trip.get("direction_id", "N/A")
                        headsign = trip.get("trip_headsign", "Unknown")

                        vehicle_type = type_map.get(str(route_type), "Unknown")

                        print(
                            f"[{datetime.now(timezone.utc)}] "
                            f"Vehicle: {v.vehicle.id} | "
                            f"Line: {route_name} | "
                            f"Type: {vehicle_type} | "
                            f"Direction({direction_id}): {headsign} | "
                            f"Color: #{route_color} | "
                            f"Lat: {v.position.latitude:.5f} | "
                            f"Lon: {v.position.longitude:.5f}"
                        )

            await asyncio.sleep(5)


# ================= ENTRY =================
if __name__ == "__main__":
    asyncio.run(run())