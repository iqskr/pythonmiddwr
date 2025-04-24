import os
import json
import zipfile
import pandas as pd
from datetime import datetime
from typing import List, Optional
from math import radians, sin, cos, sqrt, atan2

def parse_timestamp(ts):
    """Parses a timestamp into a naive datetime (tzinfo removed)."""
    if isinstance(ts, datetime):
        return ts.replace(tzinfo=None)
    try:
        return pd.to_datetime(ts).replace(tzinfo=None)
    except Exception as e:
        raise ValueError("Unknown timestamp format: " + str(ts)) from e

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculates the great-circle distance (in meters) between two coordinates."""
    R = 6371000  # Earth radius in meters
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

def make_paginated_response(df: pd.DataFrame) -> dict:
    """Converts a DataFrame to a paginated JSON response."""
    data = df.to_dict(orient="records")
    return {
        "total": len(data),
        "page": 1,
        "per_page": 100,
        "data": data
    }

def find_optional_file(zf: zipfile.ZipFile, alternatives: List[str]) -> Optional[str]:
    """Returns the first filename found in the zip that matches an alternative."""
    names = zf.namelist()
    for fname in alternatives:
        if fname in names:
            return fname
    return None
