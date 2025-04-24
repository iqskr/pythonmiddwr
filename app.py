# app.py
import os, threading, time, logging
from flask import Flask, send_from_directory, jsonify
from flask_cors import CORS
from transit_system import TransitSystem

# ——— CONFIGURATION ———
DATA_DIR   = os.path.abspath("data")
STATIC_DIR = os.path.join(DATA_DIR, "static")
LOG_PATH   = os.path.join(DATA_DIR, "app.log")
os.makedirs(STATIC_DIR, exist_ok=True)

# ——— LOGGING ———
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(message)s"
)

# ——— FLASK SETUP ———
app = Flask(__name__)
CORS(app)  # allow all origins

# Serve GTFS CSVs as static files (we’ll return them as JSON downstream)
@app.route("/static/<path:filename>")
def static_files(filename):
    return send_from_directory(STATIC_DIR, filename)

# Health‑check
@app.route("/health")
def health():
    return jsonify(status="ok")

# Real‑time endpoints (wrap your TransitSystem methods)
@app.route("/realtime/vehicle_positions.json")
def vehicle_positions():
    return jsonify(app.system.get_vehicle_positions())

@app.route("/realtime/trip_updates.json")
def trip_updates():
    return jsonify(app.system.get_trip_updates())

# Full archive endpoint
@app.route("/trip_updates_archive.json")
def trip_updates_archive():
    return jsonify(app.system.get_archive())

# Manual download‑and‑extract trigger
@app.route("/run-download")
def run_download():
    try:
        app.system.download_and_extract_zip(DATA_DIR)   # you may need to add this helper
        return jsonify(result="downloaded"), 200
    except Exception as e:
        logging.exception("Error in /run-download")
        return jsonify(error=str(e)), 500

# ——— BACKGROUND THREAD ———
def background_loop(system: TransitSystem):
    """Continuously maintain GTFS and process vehicles."""
    loop = system.get_event_loop()
    loop.run_until_complete(system.maintain_gtfs())
    while True:
        loop.run_until_complete(system.process_vehicles())
        time.sleep(1)  # match your PROCESS_INTERVAL

if __name__ == "__main__":
    # 1. Initialize and attach to Flask
    system = TransitSystem()
    app.system = system

    # 2. Start background worker
    t = threading.Thread(target=background_loop, args=(system,), daemon=True)
    t.start()

    # 3. Launch Flask on port 8000
    app.run(host="0.0.0.0", port=8000)
