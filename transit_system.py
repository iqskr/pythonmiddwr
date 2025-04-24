import os
import time
import asyncio
import aiohttp
import zipfile
import signal
import platform
import logging
import json
import pandas as pd
import numpy as np
import pymongo
import hashlib
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from math import radians
from sklearn.neighbors import BallTree
from typing import List, Dict, Optional
from pymongo.errors import PyMongoError
from tenacity import retry, wait_exponential, stop_after_attempt
from logging.handlers import TimedRotatingFileHandler

from utils import parse_timestamp, haversine_distance, make_paginated_response, find_optional_file
from config import MONGO_URL, GTFS_URL, PROCESS_INTERVAL, DEBUG_MODE

# Set PROCESS_INTERVAL to 1 second for near-real-time updates (adjustable via config)
PROCESS_INTERVAL = 1  

# Configure logging with a timed rotating file handler (rotate every 20 minutes)
handler = TimedRotatingFileHandler("transit.log", when="M", interval=20, backupCount=5)
stream_handler = logging.StreamHandler()
logging.basicConfig(
    level=logging.DEBUG if DEBUG_MODE else logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(message)s',
    handlers=[handler, stream_handler]
)
logger = logging.getLogger("TransitSystem")


class TransitSystem:
    def __init__(self):
        self.running = True
        self.mongo_client = self._connect_mongo()
        self.db = self.mongo_client.get_database("busTracking")
        self.gps_collection = self.db["locations"]
        self.gtfs_data = {}
        self.stop_tree: Optional[BallTree] = None
        self.stop_ids: Optional[np.ndarray] = None
        self.last_refresh = 0
        self.gtfs_ready = asyncio.Event()
        self.latest_feed = {}
        # For historical archive, we now store a summary per bus.
        self.trip_history_cache: Dict[str, Dict] = {}
        self.last_trip_history_update = datetime.min
        self._register_signal_handlers()

    def _register_signal_handlers(self):
        if platform.system() != "Windows":
            loop = asyncio.get_running_loop()
            for signame in ('SIGINT', 'SIGTERM'):
                loop.add_signal_handler(getattr(signal, signame), self._shutdown)

    def _shutdown(self):
        logger.critical("Initiating graceful shutdown...")
        self.running = False
        self.mongo_client.close()
        for task in asyncio.all_tasks():
            task.cancel()

    @retry(wait=wait_exponential(multiplier=1, min=2, max=30),
           stop=stop_after_attempt(5), reraise=True)
    def _connect_mongo(self):
        return pymongo.MongoClient(
            MONGO_URL,
            appname="TransitSystem",
            retryWrites=True,
            socketTimeoutMS=30000,
            connectTimeoutMS=30000,
            serverSelectionTimeoutMS=30000
        )

    async def maintain_gtfs(self):
        while self.running:
            try:
                if not os.path.exists("gtfs.zip") or time.time() - self.last_refresh > 3600:
                    await self._download_gtfs()
                self._process_gtfs()
                self._build_spatial_index()
                self.gtfs_ready.set()
                self.last_refresh = time.time()
                await asyncio.sleep(3600)
            except Exception as e:
                logger.error(f"GTFS maintenance failed: {str(e)}")
                await asyncio.sleep(300)

    @retry(wait=wait_exponential(multiplier=1, min=2, max=60),
           stop=stop_after_attempt(5))
    async def _download_gtfs(self):
        logger.info(f"GTFS URL: {GTFS_URL}")
        logger.info("Downloading GTFS data...")
        try:
            headers = {"User-Agent": "Mozilla/5.0 (compatible; TransitSystem/1.0)"}
            async with aiohttp.ClientSession(headers=headers, connector=aiohttp.TCPConnector(ssl=False)) as session:
                async with session.get(GTFS_URL, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    logger.debug(f"Response status: {response.status}")
                    response.raise_for_status()
                    content = await response.read()
            with open("gtfs.zip", "wb") as f:
                f.write(content)
            logger.info("GTFS zip file downloaded.")
        except Exception as e:
            logger.error(f"GTFS download failed: {GTFS_URL} with error: {e}")
            raise

    def _process_gtfs(self):
        with zipfile.ZipFile("gtfs.zip", "r") as zf:
            required_files = ["stops.txt", "trips.txt", "stop_times.txt", "routes.txt", "shapes.txt"]
            for f in required_files:
                if f not in zf.namelist():
                    raise FileNotFoundError(f"Missing required GTFS file: {f}")
            self.gtfs_data["stops"] = pd.read_csv(zf.open("stops.txt"), dtype={'stop_id': 'string'})
            self.gtfs_data["trips"] = pd.read_csv(zf.open("trips.txt"), dtype={'trip_id': 'string', 'route_id': 'string', 'service_id': 'string'})
            self.gtfs_data["stop_times"] = pd.read_csv(zf.open("stop_times.txt"), dtype={'trip_id': 'string'})
            self.gtfs_data["routes"] = pd.read_csv(zf.open("routes.txt"), dtype={'route_id': 'string', 'agency_id': 'string'})
            self.gtfs_data["shapes"] = pd.read_csv(zf.open("shapes.txt"), dtype={'shape_id': 'string'})
            optional_files = {
                "agencies": ["agencies.txt", "agency.txt"],
                "calendars": ["calendars.txt", "calendar.txt"],
                "calendar_dates": ["calendar_dates.txt"],
                "fare_attributes": ["fare_attributes.txt"],
                "fare_rules": ["fare_rules.txt"],
                "feed_info": ["feed_info.txt"],
                "frequencies": ["frequencies.txt"],
                "transfers": ["transfers.txt"],
                "retail_locations": ["retail_locations.txt"]
            }
            for key, alternatives in optional_files.items():
                fname = find_optional_file(zf, alternatives)
                if fname:
                    try:
                        self.gtfs_data[key] = pd.read_csv(zf.open(fname))
                        logger.debug(f"Loaded {len(self.gtfs_data[key])} records from {fname}")
                    except Exception as e:
                        logger.warning(f"Error processing {fname}: {e}")
                        self.gtfs_data[key] = pd.DataFrame()
                else:
                    self.gtfs_data[key] = pd.DataFrame()
        logger.info(f"Processed {len(self.gtfs_data['stops'])} stops from GTFS.")

    def _build_spatial_index(self):
        stops = self.gtfs_data.get("stops")
        if stops is None or stops.empty:
            raise ValueError("GTFS stops dataset is empty.")
        coords = np.radians(stops[["stop_lat", "stop_lon"]].values)
        self.stop_tree = BallTree(coords, metric='haversine', leaf_size=40)
        self.stop_ids = stops["stop_id"].values
        logger.info(f"Built spatial index with {len(stops)} stops.")

    def _validate_vehicle(self, vehicle: Dict) -> bool:
        required = {'busNumber', 'latitude', 'longitude', 'timestamp'}
        return all(vehicle.get(k) is not None for k in required)

    def _load_trip_history(self) -> Dict[str, Dict]:
        """
        Load historical trip assignment data from archive.
        For each bus, we archive a summary: bus id, list of stops with timestamps,
        final trip assignment and route.
        The in-memory cache refreshes every 10 minutes.
        """
        if datetime.now() - self.last_trip_history_update < timedelta(minutes=10):
            return self.trip_history_cache

        history_file = "trip_updates_archive.csv"
        new_history = {}
        if os.path.exists(history_file):
            try:
                df = pd.read_csv(history_file)
                df["feed_timestamp"] = pd.to_datetime(df["feed_timestamp"], errors="coerce")
                cutoff = datetime.now() - timedelta(days=2)
                recent = df[df["feed_timestamp"] >= cutoff]
                # Each row's "feed" is now a JSON list of summaries.
                for _, row in recent.iterrows():
                    summaries = json.loads(row["feed"])
                    # Ensure summaries is a list.
                    if isinstance(summaries, list):
                        for summary in summaries:
                            bus = summary.get("busNumber")
                            if bus:
                                # Update with the latest last_update.
                                if bus not in new_history or summary.get("last_update", "") > new_history[bus].get("last_update", ""):
                                    new_history[bus] = summary
                self.trip_history_cache = new_history
                self.last_trip_history_update = datetime.now()
            except Exception as e:
                logger.error(f"Error loading trip history CSV: {e}")
        return new_history

    def _assign_trip(self, vehicle):
        """
        Assigns trip_id, route_id, and direction_id to a vehicle.
        - Ignores vehicles with outdated GPS data (older than 5 minutes).
        - Uses GTFS `direction_id` directly.
        - Applies route filtering based on the stop's "routes_belong" field.
        """
        try:
            current_time = datetime.utcnow()
            gps_time = parse_timestamp(vehicle["timestamp"])
            if gps_time is None:
                logger.error("Failed to parse timestamp for bus %s", vehicle.get("busNumber", "Unknown"))
                return None
            gps_time = gps_time.replace(tzinfo=None)

            # Ignore vehicles that haven't updated in the last 5 minutes
            if (current_time - gps_time).total_seconds() > 300:
                logger.debug("Ignoring outdated GPS device: %s (Last update: %s)", vehicle.get("busNumber", "Unknown"), gps_time)
                return None

            # Check historical trip assignments (avoid reassigning if already set)
            history = self._load_trip_history()
            bus = str(vehicle.get("busNumber", "Unknown"))
            if bus in history:
                logger.debug("Using historical trip assignment for bus %s", bus)
                return history[bus]

            # Find the nearest stop using spatial index
            point = np.radians([[vehicle["latitude"], vehicle["longitude"]]])
            _, idx = self.stop_tree.query(point, k=1)
            if idx is None or not idx[0]:
                logger.error("No stop index found for vehicle %s", vehicle.get("busNumber", "Unknown"))
                return None
            nearest_stop_id = self.stop_ids[idx[0][0]]

            # Retrieve stop details to access "routes_belong"
            nearest_stop_rows = self.gtfs_data["stops"][self.gtfs_data["stops"]["stop_id"] == nearest_stop_id]
            if nearest_stop_rows.empty:
                logger.warning("No stop data found for stop id: %s", nearest_stop_id)
                return {"trip_id": "unknown", "route_id": "unknown", "direction_id": 0}
            nearest_stop = nearest_stop_rows.iloc[0]
            stop_routes = str(nearest_stop.get("routes_belong", ""))
            stop_routes_list = [r.strip() for r in stop_routes.split(",") if r.strip()]

            # Match with GTFS trip data
            stop_times_df = self.gtfs_data["stop_times"]
            candidate_trips = stop_times_df[stop_times_df["stop_id"] == nearest_stop_id]
            if candidate_trips is None or candidate_trips.empty:
                logger.debug("No matching trips found for stop %s", nearest_stop_id)
                return {"trip_id": "unknown", "route_id": "unknown", "direction_id": 0}

            # Rank trips based on stop sequence and arrival time
            candidate_trips = candidate_trips.copy()
            candidate_trips["arrival_dt"] = candidate_trips["arrival_time"].apply(
                lambda t: datetime.combine(current_time.date(), datetime.strptime(t.strip(), "%H:%M:%S").time()) if isinstance(t, str) else t
            )
            candidate_trips["time_diff"] = candidate_trips["arrival_dt"].apply(lambda dt: abs((dt - gps_time).total_seconds()))
            candidate_trips["stop_sequence"] = candidate_trips["stop_sequence"].astype(int)
            weight = 0.1
            candidate_trips["score"] = candidate_trips["time_diff"] / 60.0 - weight * candidate_trips["stop_sequence"]

            # Apply a penalty for candidate trips whose route is not in the stop's allowed routes
            trips_df = self.gtfs_data["trips"]
            def route_penalty(trip_id):
                matching_trip = trips_df[trips_df["trip_id"] == trip_id]
                if matching_trip.empty:
                    return 1000
                candidate_route = matching_trip["route_id"].iloc[0]
                return 0 if candidate_route in stop_routes_list else 1000

            candidate_trips["penalty"] = candidate_trips["trip_id"].apply(route_penalty)
            candidate_trips["score"] += candidate_trips["penalty"]

            # Select the candidate with the lowest score
            chosen = candidate_trips.sort_values("score").iloc[0]
            trip_id = chosen["trip_id"]

            matching_trips = trips_df[trips_df["trip_id"] == trip_id]
            if matching_trips.empty:
                logger.debug("Trip ID %s not found in GTFS trip data.", trip_id)
                return {"trip_id": "unknown", "route_id": "unknown", "direction_id": 0}

            route_id = matching_trips["route_id"].values[0]
            direction_id = matching_trips["direction_id"].values[0]

            return {"trip_id": trip_id, "route_id": route_id, "direction_id": direction_id}

        except Exception as e:
            logger.error("Trip assignment failed: %s", e)
            return {"trip_id": "unknown", "route_id": "unknown", "direction_id": 0}

    def _assign_trip_and_estimate(self, vehicle):
        """
        Assigns trip, estimates arrival, and adds last_update and next_stop.
        Also sets inbound/outbound based on GTFS direction_id.
        """
        assignment = self._assign_trip(vehicle)
        if assignment is None:
            return None

        try:
            # Find next stop using spatial index
            point = np.radians([[vehicle["latitude"], vehicle["longitude"]]])
            _, idx = self.stop_tree.query(point, k=1)
            if idx is None or not idx[0]:
                logger.error("Failed to retrieve next stop index for vehicle %s", vehicle.get("busNumber", "Unknown"))
                next_stop_id = None
            else:
                next_stop_id = self.stop_ids[idx[0][0]]
        except Exception as e:
            logger.error("Error finding next stop: %s", e)
            next_stop_id = None

        if next_stop_id:
            next_stop_row = self.gtfs_data["stops"][self.gtfs_data["stops"]["stop_id"] == next_stop_id]
            if next_stop_row is not None and not next_stop_row.empty:
                next_stop = next_stop_row.iloc[0]
                eta_info = self._estimate_arrival(vehicle, next_stop)
                assignment.update(eta_info)
                assignment["next_stop"] = {"stop_id": next_stop["stop_id"], "stop_name": next_stop.get("stop_name", "Unknown")}

        # Add the last update from the GPS device
        assignment["last_update"] = vehicle.get("timestamp", "")

        # Use GTFS direction_id directly; mark inbound/outbound accordingly
        direction_id = assignment.get("direction_id", 0)
        assignment["inbound_outbound"] = "inbound" if direction_id == 1 else "outbound"

        # Also add busNumber for historical archiving
        assignment["busNumber"] = vehicle.get("busNumber", "unknown")

        vehicle.update(assignment)
        return vehicle

    def _estimate_arrival(self, vehicle: Dict, next_stop: pd.Series, avg_speed_mps: float = 2.5) -> Dict:
        lat_vehicle, lon_vehicle = vehicle["latitude"], vehicle["longitude"]
        lat_stop, lon_stop = next_stop["stop_lat"], next_stop["stop_lon"]
        distance = haversine_distance(lat_vehicle, lon_vehicle, lat_stop, lon_stop)
        eta_seconds = distance / avg_speed_mps if avg_speed_mps > 0 else float('inf')
        current_time = parse_timestamp(vehicle["timestamp"]).replace(tzinfo=None)
        estimated_arrival = current_time + timedelta(seconds=eta_seconds)
        return {"distance_m": round(distance, 1), "estimated_arrival": estimated_arrival.isoformat()}

    def _group_by_bus(self, vehicles: List[Dict]) -> List[Dict]:
        grouped = {}
        for v in vehicles:
            bus = str(v.get("busNumber", "unknown"))
            try:
                ts = parse_timestamp(v["timestamp"])
            except Exception:
                continue
            if bus not in grouped or parse_timestamp(grouped[bus]["timestamp"]) < ts:
                grouped[bus] = v
        return list(grouped.values())

    def _archive_feed_to_csv(self, feed_data: dict):
        """
        Archive a summarized feed per bus: busNumber, stops visited with timestamps,
        final trip assignment and route.
        """
        archive_file = "trip_updates_archive.csv"
        # Build a simple summary
        summary = {}
        for entity in feed_data.get("entity", []):
            bus = entity.get("id")
            trip_info = entity.get("vehicle", {}).get("trip", {})
            next_stop = entity.get("next_stop", {})
            timestamp = entity.get("vehicle", {}).get("position", {}).get("timestamp", "")
            if bus not in summary:
                summary[bus] = {
                    "busNumber": bus,
                    "stops": [],
                    "first_update": timestamp,
                    "last_update": timestamp,
                    "trip_id": trip_info.get("trip_id", ""),
                    "route_id": trip_info.get("route_id", "")
                }
            else:
                summary[bus]["last_update"] = timestamp
            if next_stop:
                summary[bus]["stops"].append(next_stop)
        archived = list(summary.values())
        new_row = {
            "feed_timestamp": datetime.now().isoformat(),
            "feed": json.dumps(archived)
        }
        if os.path.exists(archive_file):
            try:
                df = pd.read_csv(archive_file)
                df["feed_timestamp"] = pd.to_datetime(df["feed_timestamp"], errors="coerce")
                df = df[df["feed_timestamp"] >= (datetime.now() - timedelta(days=3))]
            except Exception as e:
                logger.error(f"Error reading archive CSV: {e}")
                df = pd.DataFrame()
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        else:
            df = pd.DataFrame([new_row])
        try:
            df.to_csv(archive_file, index=False)
            logger.debug("Realtime feed archived to CSV file.")
        except Exception as e:
            logger.error(f"Error writing to archive CSV: {e}")

    def get_trip_updates_archive(self) -> dict:
        archive_file = "trip_updates_archive.csv"
        if not os.path.exists(archive_file):
            return {"error": "No archive data available."}
        try:
            df = pd.read_csv(archive_file)
            df["feed_timestamp"] = pd.to_datetime(df["feed_timestamp"], errors="coerce")
            cutoff = datetime.now() - timedelta(days=3)
            recent = df[df["feed_timestamp"] >= cutoff]
            data = recent.sort_values("feed_timestamp", ascending=False).to_dict(orient="records")
            return {"total": len(data), "data": data}
        except Exception as e:
            logger.error(f"Error reading trip updates archive: {e}")
            return {"error": "Failed to load archive data."}

    def _create_json_feed(self, vehicles: List[Dict]) -> dict:
        feed = {
            "header": {
                "gtfs_realtime_version": "2.0",
                "incrementality": "FULL_DATASET",
                "timestamp": int(time.time())
            },
            "entity": []
        }
        for vehicle in vehicles:
            try:
                busNumber = str(vehicle.get("busNumber", "unknown"))
                trip_id = str(vehicle.get("trip_id", "unknown"))
                route_id = str(vehicle.get("route_id", "unknown"))
                direction_id = int(vehicle.get("direction_id", 0))
                deviceId = str(vehicle.get("deviceId", "unknown"))
                latitude = float(vehicle["latitude"]) if vehicle["latitude"] is not None else 0.0
                longitude = float(vehicle["longitude"]) if vehicle["longitude"] is not None else 0.0
                speed = float(vehicle.get("speed") or 0)
                timestamp_value = int(parse_timestamp(vehicle["timestamp"]).timestamp())
            except Exception as e:
                logger.error(f"Error processing vehicle data for JSON feed: {e}")
                continue

            entity = {
                "id": busNumber,
                "vehicle": {
                    "trip": {
                        "trip_id": trip_id,
                        "route_id": route_id,
                        "direction_id": direction_id
                    },
                    "position": {
                        "latitude": latitude,
                        "longitude": longitude,
                        "speed": speed,
                        "timestamp": timestamp_value
                    },
                    "vehicle": {
                        "id": deviceId,
                        "label": busNumber
                    },
                    "occupancy_status": str(vehicle.get("occupancy_status", "NO_DATA_AVAILABLE")),
                    "occupancy_percentage": int(vehicle.get("occupancy_percentage", 0))
                },
                "next_stop": vehicle.get("next_stop", {}),
                "distance_m": vehicle.get("distance_m", None),
                "estimated_arrival": vehicle.get("estimated_arrival", None),
                "last_update": vehicle.get("timestamp", "")
            }
            feed["entity"].append(entity)
        return feed

    def _parallel_process(self, vehicles: List[Dict]) -> List[Dict]:
        num_cpus = os.cpu_count() or 1
        chunk_size = max(1, len(vehicles) // num_cpus)
        chunks = [vehicles[i:i + chunk_size] for i in range(0, len(vehicles), chunk_size)]
        processed = []
        for chunk in chunks:
            processed.extend(self._process_chunk(chunk))
        return processed

    def _process_chunk(self, chunk: List[Dict]) -> List[Dict]:
        processed_chunk = []
        for v in chunk:
            if not self._validate_vehicle(v):
                continue
            assignment = self._assign_trip_and_estimate(v)
            if assignment is not None:
                processed_chunk.append(assignment)
        return processed_chunk

    async def process_vehicles(self):
        await self.gtfs_ready.wait()
        executor = ThreadPoolExecutor(max_workers=os.cpu_count())
        loop = asyncio.get_event_loop()
        while self.running:
            try:
                vehicles = list(self.gps_collection.find().limit(1000))
                if not vehicles:
                    await asyncio.sleep(1)
                    continue
                processed_vehicles = await loop.run_in_executor(executor, self._parallel_process, vehicles)
                processed_vehicles = self._group_by_bus(processed_vehicles)
                json_feed = self._create_json_feed(processed_vehicles)
                self.latest_feed = json_feed
                self._archive_feed_to_csv(json_feed)
                logger.info(f"Processed and updated feed for {len(processed_vehicles)} buses.")
                await asyncio.sleep(float(PROCESS_INTERVAL))
            except PyMongoError as e:
                logger.error(f"MongoDB error: {str(e)}")
                await self._handle_db_error()
            except Exception as e:
                logger.error(f"Processing error: {str(e)}")
                await asyncio.sleep(1)

    async def _handle_db_error(self):
        logger.warning("Reconnecting to MongoDB...")
        try:
            self.mongo_client = self._connect_mongo()
            self.db = self.mongo_client.get_database("busTracking")
            self.gps_collection = self.db["locations"]
            logger.info("MongoDB reconnected successfully.")
        except Exception as e:
            logger.critical(f"Failed to reconnect: {str(e)}")
            await asyncio.sleep(30)
