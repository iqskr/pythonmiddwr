import os
import time
import json
import pandas as pd
from aiohttp import web
import logging
from transit_system import TransitSystem

logger = logging.getLogger("TransitSystem")

def clean_json_string(s: str) -> str:
    if s.startswith('"') and s.endswith('"'):
        s = s[1:-1]
    return s.replace('""', '"')

# --- CORS Middleware ---
@web.middleware
async def cors_middleware(request, handler):
    try:
        response = await handler(request)
    except web.HTTPException as ex:
        response = ex
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "*"
    return response

# --- Handlers ---
async def index_handler(request):
    endpoints = [
        "/",
        "/agencies.json",
        "/calendar_dates.json",
        "/calendars.json",
        "/routes.json",
        "/shapes.json",
        "/stops.json",
        "/stop_times.json",
        "/trips.json",
        "/realtime/vehicle_positions.json",
        "/realtime/trip_updates.json",
        "/trip_updates_archive.json"
    ]
    return web.json_response({"endpoints": endpoints}, dumps=lambda s: json.dumps(s, indent=2))

async def realtime_generic_handler(request):
    system = request.app["system"]
    data = system.latest_feed if system.latest_feed else {"header": {}, "entity": []}
    return web.json_response(data, dumps=lambda s: json.dumps(s, indent=2))

async def static_handler(request, key):
    system = request.app["system"]
    df = system.gtfs_data.get(key)
    if df is None or df.empty:
        return web.json_response({"error": f"GTFS {key} data not available."}, status=503)
    from utils import make_paginated_response
    data = make_paginated_response(df)
    return web.json_response(data, dumps=lambda s: json.dumps(s, indent=2))

async def trip_updates_archive_handler(request):
    archive_file = "trip_updates_archive.csv"
    if not os.path.exists(archive_file):
        return web.json_response({"error": "No archive data available."}, status=404)
    try:
        df = pd.read_csv(archive_file, parse_dates=["feed_timestamp"])
        def decode_feed(x):
            try:
                cleaned = clean_json_string(x) if isinstance(x, str) else x
                return json.loads(cleaned)
            except Exception as e:
                logger.error(f"Error decoding JSON feed: {e}")
                return x
        df["feed"] = df["feed"].apply(decode_feed)
        records = df.to_dict(orient="records")
        return web.json_response(records, dumps=lambda s: json.dumps(s, indent=2, default=str))
    except Exception as e:
        logger.error(f"Error reading archive CSV: {e}")
        return web.json_response({"error": str(e)}, status=500)

async def start_http_server(system: TransitSystem):
    app = web.Application(middlewares=[cors_middleware])
    app["system"] = system

    app.router.add_get('/', index_handler)
    app.router.add_get('/agencies.json', lambda req: static_handler(req, "agencies"))
    app.router.add_get('/calendar_dates.json', lambda req: static_handler(req, "calendar_dates"))
    app.router.add_get('/calendars.json', lambda req: static_handler(req, "calendars"))
    app.router.add_get('/routes.json', lambda req: static_handler(req, "routes"))
    app.router.add_get('/shapes.json', lambda req: static_handler(req, "shapes"))
    app.router.add_get('/stops.json', lambda req: static_handler(req, "stops"))
    app.router.add_get('/stop_times.json', lambda req: static_handler(req, "stop_times"))
    app.router.add_get('/trips.json', lambda req: static_handler(req, "trips"))
    app.router.add_get('/realtime/vehicle_positions.json', realtime_generic_handler)
    app.router.add_get('/realtime/trip_updates.json', realtime_generic_handler)
    app.router.add_get('/trip_updates_archive.json', trip_updates_archive_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    await site.start()
    logger.info("HTTP server started on port 8080 (CORS enabled)")
