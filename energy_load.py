import requests
import pandas as pd
from datetime import datetime, timedelta, time
from astral import LocationInfo
from astral.sun import sun
import math
from astral import LocationInfo
from astral.sun import sunrise, sunset
import os
from datetime import datetime


API_URL = "https://api.skyelectric.com/api"  # GraphQL endpoint
TOKEN = "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE4MDUwOTU3NzgsImxhbmd1YWdlIjoiZW4iLCJyb2xlIjoiTk9DX1ZJRVdFUiIsInN1YiI6IjgwOGZlODJlLWFjYjYtNGFlNi1hNmM3LTYxZGRjOWM2MDgzNyIsInR6IjowLCJ1dCI6MH0.ZlxoBwclHF6SHjoyyNCN_HAO4lBkfP98ftRwnh_HM13zqAoX7RUM-56kDITu19tkk8v5n6vbFlxTrfgSSCxvOw"
SYSTEM_ID = "c0aeb95e-033a-4c9d-8a49-35697de9df82"
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

# Islamabad coordinates
CITY_NAME = "Islamabad"
LAT = 33.6844
LON = 73.0479
TIMEZONE = "Asia/Karachi"

# --- GraphQL queries ---
HOURLY_ENERGY_QUERY = """
query hourlyEnergy($systemId: ID!, $startTimestamp: Long!, $endTimestamp: Long!) {
  systemHourlyEnergyStats(systemId: $systemId, startTimestamp: $startTimestamp, endTimestamp: $endTimestamp) {
    values {
      time
      load
      pvProduced
      pvExported
      gridConsumed
    }
  }
}
"""

def fetch_data(query, variables):
    resp = requests.post(API_URL, json={"query": query, "variables": variables}, headers=HEADERS)
    resp.raise_for_status()
    json_data = resp.json()
    if "errors" in json_data:
        print("API returned errors:", json_data["errors"])
        return None
    return json_data.get("data")

# def unix_to_datetime(ms):
#     return datetime.utcfromtimestamp(ms / 1000)
def unix_to_datetime(ms):
    # Convert UTC -> Pakistan Time (+5 hours)
    return datetime.utcfromtimestamp(ms / 1000) + timedelta(hours=0)

# --- Time range and execution moved into function for reuse ---
def run_energy_load(system_id, start_date, end_date, out_dir='.'):
    os.makedirs(out_dir, exist_ok=True)
    try:
        start_dt = datetime.fromisoformat(start_date)
    except Exception:
        start_dt = datetime(2024, 1, 1)
    try:
        end_dt = datetime.fromisoformat(end_date)
    except Exception:
        end_dt = datetime.utcnow()

    start_ts = int(start_dt.timestamp() * 1000)
    end_ts = int(end_dt.timestamp() * 1000)

    variables = {
        "systemId": system_id,
        "startTimestamp": start_ts,
        "endTimestamp": end_ts
    }

    # --- Fetch hourly energy ---
    energy_data = fetch_data(HOURLY_ENERGY_QUERY, variables)
    if energy_data is None:
        raise SystemExit("Failed to fetch energy data.")

    # --- Process hourly energy ---
    energy_list = energy_data["systemHourlyEnergyStats"]["values"]
    energy_df = pd.DataFrame(energy_list)
    energy_df["datetime"] = energy_df["time"].apply(unix_to_datetime)

    # --- PREPROCESS: remove duplicate hourly rows ---
    # Some systems produce two rows per hour: one at HH:00:00 (often zero) and one at HH:00:01 (the correct value).
    # For each hour keep the last non-zero row if available, otherwise keep one zero row.
    energy_df["hour"] = energy_df["datetime"].dt.floor("H")

    def is_all_zero(row):
        # Consider common energy fields; default to 0 when absent
        return (
            (row.get("load", 0) == 0) and
            (row.get("pvProduced", 0) == 0) and
            (row.get("pvExported", 0) == 0) and
            (row.get("gridConsumed", 0) == 0)
        )

    cleaned_rows = []
    for hour, group in energy_df.groupby("hour"):
        if len(group) == 1:
            cleaned_rows.append(group.iloc[0])
            continue

        zero_rows = group[group.apply(is_all_zero, axis=1)]
        non_zero_rows = group[~group.apply(is_all_zero, axis=1)]

        if not non_zero_rows.empty:
            # keep last valid row (usually HH:00:01)
            cleaned_rows.append(non_zero_rows.sort_values("datetime").iloc[-1])
        else:
            # all-zero → keep the first one
            cleaned_rows.append(zero_rows.iloc[0])

    energy_df = pd.DataFrame(cleaned_rows).sort_values("datetime").reset_index(drop=True)

    # recompute date after cleaning
    energy_df["date"] = energy_df["datetime"].dt.date

    # --- Calculate sunrise/sunset for each day using Astral ---
    location = LocationInfo(name=CITY_NAME, region="Pakistan", timezone=TIMEZONE, latitude=LAT, longitude=LON)

    sunrise_dict = {}
    sunset_dict = {}
    for day in energy_df["date"].unique():
        try:
            sr = sunrise(location.observer, date=day)
            ss = sunset(location.observer, date=day)
            # Astral may return UTC or naive datetimes; ensure times are Pakistan local time by adding +5h
            try:
                sr = sr + timedelta(hours=5)
                ss = ss + timedelta(hours=5)
            except Exception:
                # if addition fails, leave as-is
                pass
            sunrise_dict[day] = sr.time()
            sunset_dict[day] = ss.time()
        except Exception as e:
            # fallback to 6AM / 6PM if Astral fails
            sunrise_dict[day] = time(6, 0, 0)
            sunset_dict[day] = time(18, 0, 0)
            print(f"Warning: Astral failed for {day}, using default 6AM/6PM: {e}")

    # Map sunrise/sunset to each row
    energy_df["sunrise"] = energy_df["date"].map(sunrise_dict)
    energy_df["sunset"] = energy_df["date"].map(sunset_dict)

    # --- Save raw hourly CSV ---
    raw_csv = os.path.join(out_dir, f"{system_id}_raw_energy_load.csv")
    energy_df[["datetime", "load", "sunrise", "sunset"]].to_csv(raw_csv, index=False)
    print(f"Raw hourly CSV saved: {raw_csv}")

    # --- Daily aggregation ---
    daily_summary = []
    for day, group in energy_df.groupby("date"):
        total_load = group["load"].sum()
        night_load = group[(group["datetime"].dt.time < group["sunrise"].iloc[0]) |
                           (group["datetime"].dt.time > group["sunset"].iloc[0])]["load"].sum()
        night_fraction = round(night_load / total_load * 100, 2) if total_load > 0 else 0
        daily_summary.append({
            "date": day,
            "total_load": total_load,
            "night_load": night_load,
            "night_fraction": night_fraction
        })

    daily_df = pd.DataFrame(daily_summary)
    daily_csv = os.path.join(out_dir, f"{system_id}_daily_energy_summary.csv")
    daily_df.to_csv(daily_csv, index=False)
    print(f"Daily summary CSV saved: {daily_csv}")

    return {
        'raw_csv': raw_csv,
        'daily_csv': daily_csv
    }

import yaml
if __name__ == '__main__':
    import argparse
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--system-id', required=True)
    # parser.add_argument('--start', default='2024-01-01')
    # parser.add_argument('--end', default=datetime.utcnow().strftime('%Y-%m-%d'))
    # parser.add_argument('--out-dir', default='.')
    # args = parser.parse_args()
    with open("conf.yaml","r") as f:
        config = yaml.safe_load(f)

    defaults = config.get("defaults",{})
    system_id = defaults.get("system_id")
    start_date = defaults.get("start_date")
    end_date = defaults.get("end_date")
    out_dir = "."
    run_energy_load(system_id, start_date, end_date, out_dir)