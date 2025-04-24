import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env

# Environment settings
MONGO_URL = os.getenv("MONGO_URL", "mongodb+srv://jadhavh655:HaRsHalJaDHav@cluster0.54der.mongodb.net/busTracking?retryWrites=true&w=majority&appName=Cluster0")
GTFS_URL = os.getenv("GTFS_URL", "https://msrtctransit.multiscreensite.com/gtfs/gtfs.zip")
PROCESS_INTERVAL = int(os.getenv("PROCESS_INTERVAL", "10"))
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"


