import requests
import pandas as pd
from datetime import datetime
import os
import yaml


API_URL = "https://api.skyelectric.com/api"  # GraphQL endpoint
TOKEN = "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE4MDUwOTU3NzgsImxhbmd1YWdlIjoiZW4iLCJyb2xlIjoiTk9DX1ZJRVdFUiIsInN1YiI6IjgwOGZlODJlLWFjYjYtNGFlNi1hNmM3LTYxZGRjOWM2MDgzNyIsInR6IjowLCJ1dCI6MH0.ZlxoBwclHF6SHjoyyNCN_HAO4lBkfP98ftRwnh_HM13zqAoX7RUM-56kDITu19tkk8v5n6vbFlxTrfgSSCxvOw"


# --- CONFIG ---
#TOKEN = "YOUR_BEARER_TOKEN_HERE"
# SYSTEM_ID = "c0aeb95e-033a-4c9d-8a49-35697de9df82"
#API_URL = "https://japp-api.skyelectric.com/api"

# HEADERS = {
#     "Authorization": f"Bearer {TOKEN}",
#     "Content-Type": "application/json"
# }

# # --- GraphQL queries ---
# DAILY_WEATHER_QUERY = """
# query dailyWeather($systemId: ID!) {
#   dailyWeatherApp(systemId: $systemId) {
#     time
#     predCloudPercent
#     sunrise
#     sunset
#     __typename
#   }
# }
# """

# DAILY_ENERGY_QUERY = """
# query energyStats($systemId: ID!) {
#   systemDailyEnergyStats(systemId: $systemId) {
#     values {
#       time
#       pvProduced
#       pvExported
#       gridConsumed
#       __typename
#     }
#     __typename
#   }
# }
# """

# # def fetch_data(query):
# #     payload = {"query": query, "variables": {"systemId": SYSTEM_ID}}
# #     resp = requests.post(API_URL, json=payload, headers=HEADERS)
# #     resp.raise_for_status()
# #     return resp.json()
# def fetch_data(query):
#     payload = {"query": query, "variables": {"systemId": SYSTEM_ID}}
#     resp = requests.post(API_URL, json=payload, headers=HEADERS)
#     resp.raise_for_status()  # Will raise if status != 200
#     json_data = resp.json()

#     if "errors" in json_data:
#         print("API returned errors:", json_data["errors"])
#         return None
#     if "data" not in json_data:
#         print("API response missing 'data':", json_data)
#         return None

#     return json_data["data"]

# def unix_to_datetime(ms):
#     return datetime.utcfromtimestamp(ms / 1000)

# # --- Fetch data ---
# weather_json = fetch_data(DAILY_WEATHER_QUERY)
# energy_json = fetch_data(DAILY_ENERGY_QUERY)

# # --- Process daily data ---
# weather_list = weather_json["data"]["dailyWeatherApp"]
# energy_list = energy_json["data"]["systemDailyEnergyStats"]["values"]

# # Merge by 'time'
# weather_df = pd.DataFrame(weather_list)
# energy_df = pd.DataFrame(energy_list)

# # Convert unix ms to datetime
# weather_df["date"] = weather_df["time"].apply(unix_to_datetime)
# weather_df["sunrise"] = weather_df["sunrise"].apply(unix_to_datetime)
# weather_df["sunset"] = weather_df["sunset"].apply(unix_to_datetime)
# energy_df["date"] = energy_df["time"].apply(unix_to_datetime)

# def to_timestamp_ms(date_str):
#     # date_str format: "YYYY-MM-DD"
#     dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
#     return int(dt.timestamp() * 1000)

# variables = {
#     "startTimestamp": to_timestamp_ms("2025-02-01"),
#     "endTimestamp": to_timestamp_ms("2026-02-28"),
#     "country": "PAKISTAN",
#     "city": "ISLAMABAD"
# }


# # Filter dates between Feb 2025 and Feb 2026
# start_date = datetime(2025, 2, 1)
# end_date = datetime(2026, 2, 28)
# weather_df = weather_df[(weather_df["date"] >= start_date) & (weather_df["date"] <= end_date)]
# energy_df = energy_df[(energy_df["date"] >= start_date) & (energy_df["date"] <= end_date)]

# # Merge daily data on date
# daily_df = pd.merge(energy_df, weather_df, on="date", how="left")

# # Rename columns
# daily_df = daily_df.rename(columns={
#     "pvProduced": "Solar Energy Produced",
#     "pvExported": "Solar Exported",
#     "gridConsumed": "Grid Consumed"
# })

# # Select columns for daily CSV
# daily_df = daily_df[["date", "Solar Energy Produced", "Solar Exported", "Grid Consumed", "sunrise", "sunset"]]

# # Save daily CSV
# daily_df.to_csv("dashboard-data-daily.csv", index=False)
# print("Daily CSV saved: dashboard-data-daily.csv")

# # --- Monthly aggregation ---
# monthly_df = daily_df.copy()
# monthly_df["month"] = monthly_df["date"].dt.to_period("M")
# monthly_agg = monthly_df.groupby("month").agg({
#     "Solar Energy Produced": "sum",
#     "Grid Consumed": "sum",
#     "Solar Exported": "sum"
# }).reset_index()

# # Compute unitsT = Grid Consumed - Solar Exported
# monthly_agg["unitsT"] = monthly_agg["Grid Consumed"] - monthly_agg["Solar Exported"]

# # Save monthly CSV
# monthly_agg.to_csv("dashboard-data-monthly.csv", index=False)
# print("Monthly CSV saved: dashboard-data-monthly.csv")


HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

# --- GraphQL queries with timestamp variables ---
DAILY_WEATHER_QUERY = """
query dailyWeather($systemId: ID!, $startTimestamp: Long!, $endTimestamp: Long!, $country: String!, $city: String!) {
  dailyWeatherApp(systemId: $systemId, startTimestamp: $startTimestamp, endTimestamp: $endTimestamp, country: $country, city: $city) {
    time
    predCloudPercent
    sunrise
    sunset
    __typename
  }
}
"""

DAILY_ENERGY_QUERY = """
query energyStats($systemId: ID!, $startTimestamp: Long!, $endTimestamp: Long!) {
  systemDailyEnergyStats(systemId: $systemId, startTimestamp: $startTimestamp, endTimestamp: $endTimestamp) {
    values {
      time
      pvProduced
      pvExported
      gridConsumed
      __typename
    }
    __typename
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

def unix_to_datetime(ms):
    return datetime.utcfromtimestamp(ms / 1000)

def run_tosee_units(system_id, start_date, end_date, out_dir='.'):
    os.makedirs(out_dir, exist_ok=True)

    # use provided system_id instead of hardcoded
    sid = system_id

    try:
        start_dt = datetime.fromisoformat(start_date)
    except Exception:
        start_dt = datetime(2025, 2, 1)
    try:
        end_dt = datetime.fromisoformat(end_date)
    except Exception:
        end_dt = datetime(2026, 2, 28)

    start_ts = int(start_dt.timestamp() * 1000)
    end_ts = int(end_dt.timestamp() * 1000)

    # --- Variables for API ---
    weather_variables = {
        "systemId": sid,
        "startTimestamp": start_ts,
        "endTimestamp": end_ts,
        "country": "PAKISTAN",
        "city": "ISLAMABAD"
    }

    energy_variables = {
        "systemId": sid,
        "startTimestamp": start_ts,
        "endTimestamp": end_ts
    }

    # --- Fetch data ---
    weather_data = fetch_data(DAILY_WEATHER_QUERY, weather_variables)
    energy_data = fetch_data(DAILY_ENERGY_QUERY, energy_variables)

    if weather_data is None or energy_data is None:
        raise SystemExit("Failed to fetch data from API.")

    # --- Process daily data ---
    weather_list = weather_data.get("dailyWeatherApp", [])
    energy_list = energy_data.get("systemDailyEnergyStats", {}).get("values", [])

    weather_df = pd.DataFrame(weather_list)
    energy_df = pd.DataFrame(energy_list)

    # Convert unix ms to datetime
    weather_df["date"] = weather_df["time"].apply(unix_to_datetime)
    weather_df["sunrise"] = weather_df["sunrise"].apply(unix_to_datetime)
    weather_df["sunset"] = weather_df["sunset"].apply(unix_to_datetime)
    energy_df["date"] = energy_df["time"].apply(unix_to_datetime)

    # Merge daily data on date
    daily_df = pd.merge(energy_df, weather_df, on="date", how="left")

    # Rename columns
    daily_df = daily_df.rename(columns={
        "pvProduced": "Solar Energy Produced",
        "pvExported": "Solar Exported",
        "gridConsumed": "Grid Consumed"
    })

    # Select columns for daily CSV
    daily_df = daily_df[["date", "Solar Energy Produced", "Solar Exported", "Grid Consumed", "sunrise", "sunset"]]

    # Save daily CSV
    daily_csv = os.path.join(out_dir, f"{sid}_dashboard-data-daily.csv")
    daily_df.to_csv(daily_csv, index=False)
    print(f"Daily CSV saved: {daily_csv}")

    # --- Monthly aggregation ---
    monthly_df = daily_df.copy()
    monthly_df["month"] = monthly_df["date"].dt.to_period("M")
    monthly_agg = monthly_df.groupby("month").agg({
        "Solar Energy Produced": "sum",
        "Grid Consumed": "sum",
        "Solar Exported": "sum"
    }).reset_index()

    # Compute unitsT = Grid Consumed - Solar Exported
    monthly_agg["unitsT"] = monthly_agg["Grid Consumed"] - monthly_agg["Solar Exported"]

    # Save monthly CSV
    monthly_csv = os.path.join(out_dir, f"{sid}_dashboard-data-monthly.csv")
    monthly_agg.to_csv(monthly_csv, index=False)
    print(f"Monthly CSV saved: {monthly_csv}")

    return {
        'daily_csv': daily_csv,
        'monthly_csv': monthly_csv
    }

if __name__ == '__main__':
    # Load config
    with open("conf.yaml", "r") as f:
        config = yaml.safe_load(f)

    defaults = config.get("defaults", {})

    system_id = defaults.get("system_id")
    start_date = defaults.get("start_date")
    end_date = defaults.get("end_date")
    out_dir = "."  # you can also add output directory to conf.yaml if needed

    if not system_id:
        raise ValueError("defaults.system_id is required in conf.yaml")

    run_tosee_units(system_id, start_date, end_date, out_dir)
# if __name__ == '__main__':
#     import argparse
#     parser = argparse.ArgumentParser()
#     parser.add_argument('--system-id', required=False)
#     parser.add_argument('--start', default='2025-02-01')
#     parser.add_argument('--end', default='2026-02-28')
#     parser.add_argument('--out-dir', default='.')
#     args = parser.parse_args()
#     run_tosee_units(args.system_id, args.start, args.end, args.out_dir)