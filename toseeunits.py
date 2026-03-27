# import requests
# import pandas as pd
# from datetime import datetime
# from datetime import time as dt_time
# import os
# import csv
# import yaml
# from pathlib import Path

# # add astral for sunrise/sunset fallback
# try:
#     from astral import LocationInfo
#     from astral.sun import sunrise, sunset
# except Exception:
#     LocationInfo = None
#     sunrise = None
#     sunset = None



# # Load config from conf.yaml to get API URL and token
# CONFIG_PATH = Path(__file__).resolve().parent / "conf.yaml"
# try:
#     with open(CONFIG_PATH, "r") as f:
#         config = yaml.safe_load(f) or {}
#     giles = config.get("giles", {})
#     tosee_cfg = giles.get("toseeunits", {})
#     API_URL = tosee_cfg.get("url") or os.getenv("GILES_API_URL")
#     TOKEN = tosee_cfg.get("token") or os.getenv("GILES_API_TOKEN")
# except Exception as e:
#     print(f"Warning: could not load conf.yaml: {e}")

# HEADERS = {
#     "Authorization": f"Bearer {TOKEN}",
#     "Content-Type": "application/json"
# }

# # --- GraphQL queries with timestamp variables ---
# DAILY_WEATHER_QUERY = """
# query dailyWeather($systemId: ID!, $startTimestamp: Long!, $endTimestamp: Long!, $country: String!, $city: String!) {
#   dailyWeatherApp(systemId: $systemId, startTimestamp: $startTimestamp, endTimestamp: $endTimestamp, country: $country, city: $city) {
#     time
#     predCloudPercent
#     sunrise
#     sunset
#     __typename
#   }
# }
# """

# DAILY_ENERGY_QUERY = """
# query energyStats($systemId: ID!, $startTimestamp: Long!, $endTimestamp: Long!) {
#   systemDailyEnergyStats(systemId: $systemId, startTimestamp: $startTimestamp, endTimestamp: $endTimestamp) {
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

# def fetch_data(query, variables):
#     resp = requests.post(API_URL, json={"query": query, "variables": variables}, headers=HEADERS)
#     resp.raise_for_status()
#     json_data = resp.json()
#     if "errors" in json_data:
#         print("API returned errors:", json_data["errors"])
#         return None
#     return json_data.get("data")

# def unix_to_datetime(ms):
#     return datetime.utcfromtimestamp(ms / 1000)

# def run_tosee_units(system_id, start_date, end_date, out_dir='.'):
#     os.makedirs(out_dir, exist_ok=True)

#     # use provided system_id instead of hardcoded
#     sid = system_id 

#     try:
#         start_dt = datetime.fromisoformat(start_date)
#     except Exception:
#         start_dt = datetime(2025, 2, 1)
#     try:
#         end_dt = datetime.fromisoformat(end_date)
#     except Exception:
#         end_dt = datetime(2026, 2, 28)

#     start_ts = int(start_dt.timestamp() * 1000)
#     end_ts = int(end_dt.timestamp() * 1000)

#     # --- Variables for API ---
#     weather_variables = {
#         "systemId": sid,
#         "startTimestamp": start_ts,
#         "endTimestamp": end_ts,
#         "country": "PAKISTAN",
#         "city": "ISLAMABAD"
#     }

#     energy_variables = {
#         "systemId": sid,
#         "startTimestamp": start_ts,
#         "endTimestamp": end_ts
#     }

#     # Also prepare hourly query variables and query
#     HOURLY_ENERGY_QUERY = """
# query hourlyEnergy($systemId: ID!, $startTimestamp: Long!, $endTimestamp: Long!) {
#   systemHourlyEnergyStats(systemId: $systemId, startTimestamp: $startTimestamp, endTimestamp: $endTimestamp) {
#     values {
#       time
#       load
#       pvProduced
#       pvExported
#       gridConsumed
#     }
#   }
# }
# """

#     # --- Fetch data ---
#     weather_data = fetch_data(DAILY_WEATHER_QUERY, weather_variables)
#     energy_data = fetch_data(DAILY_ENERGY_QUERY, energy_variables)
#     hourly_data = fetch_data(HOURLY_ENERGY_QUERY, energy_variables)

#     if weather_data is None or energy_data is None:
#         raise SystemExit("Failed to fetch daily data from API.")

#     # --- Process daily data ---
#     weather_list = weather_data.get("dailyWeatherApp", [])
#     energy_list = energy_data.get("systemDailyEnergyStats", {}).get("values", [])

#     weather_df = pd.DataFrame(weather_list)
#     energy_df = pd.DataFrame(energy_list)

#     # Convert unix ms to datetime for daily
#     weather_df["date"] = weather_df["time"].apply(unix_to_datetime)
#     # weather sunrise/sunset are unix ms timestamps; convert to datetime
#     if "sunrise" in weather_df.columns:
#         weather_df["sunrise"] = weather_df["sunrise"].apply(unix_to_datetime)
#     if "sunset" in weather_df.columns:
#         weather_df["sunset"] = weather_df["sunset"].apply(unix_to_datetime)

#     weather_df["day"] = weather_df["date"].dt.date

#     energy_df["date"] = energy_df["time"].apply(unix_to_datetime)
#     energy_df["day"] = energy_df["date"].dt.date

#     # Merge daily data on date
#     daily_df = pd.merge(energy_df, weather_df, on="date", how="left")

#     # Rename columns
#     daily_df = daily_df.rename(columns={
#         "pvProduced": "Solar Energy Produced",
#         "pvExported": "Solar Exported",
#         "gridConsumed": "Grid Consumed"
#     })

#     # Select columns for daily CSV
#     daily_out = os.path.join(out_dir, f"{sid}_dashboard-data-daily.csv")
#     daily_df = daily_df[["date", "Solar Energy Produced", "Solar Exported", "Grid Consumed", "sunrise", "sunset"]]
#     daily_df.to_csv(daily_out, index=False)
#     print(f"Daily CSV saved: {daily_out}")

#     # --- Monthly aggregation ---
#     monthly_df = daily_df.copy()
#     monthly_df["month"] = monthly_df["date"].dt.to_period("M")
#     monthly_agg = monthly_df.groupby("month").agg({
#         "Solar Energy Produced": "sum",
#         "Grid Consumed": "sum",
#         "Solar Exported": "sum"
#     }).reset_index()

#     # Compute unitsT = Grid Consumed - Solar Exported
#     monthly_agg["unitsT"] = monthly_agg["Grid Consumed"] - monthly_agg["Solar Exported"]

#     # Save monthly CSV
#     monthly_out = os.path.join(out_dir, f"{sid}_dashboard-data-monthly.csv")
#     monthly_agg.to_csv(monthly_out, index=False)
#     print(f"Monthly CSV saved: {monthly_out}")
#         # --- Hourly data processing ---
#     hourly_out = None
#     import_export_out = os.path.join(out_dir, "import-export.csv")

#     if hourly_data is not None:
#         try:
#             hourly_list = hourly_data.get("systemHourlyEnergyStats", {}).get("values", [])
#             hourly_df = pd.DataFrame(hourly_list)

#             if not hourly_df.empty:
#                 hourly_df["datetime"] = hourly_df["time"].apply(unix_to_datetime)
                
#                 # ----------------------------------
#                 # CLEAN DUPLICATE HOURLY BAD DATA
#                 # ----------------------------------

#                 # Round to hour
#                 hourly_df["hour"] = hourly_df["datetime"].dt.floor("H")

#                 def is_all_zero(row):
#                     return (
#                         (row.get("load", 0) == 0) and
#                         (row.get("pvProduced", 0) == 0) and
#                         (row.get("pvExported", 0) == 0) and
#                         (row.get("gridConsumed", 0) == 0)
#                     )

#                 cleaned_rows = []

#                 for hour, group in hourly_df.groupby("hour"):

#                     if len(group) == 1:
#                         cleaned_rows.append(group.iloc[0])
#                         continue

#                     # Separate zero and non-zero rows
#                     zero_rows = group[group.apply(is_all_zero, axis=1)]
#                     non_zero_rows = group[~group.apply(is_all_zero, axis=1)]

#                     if not non_zero_rows.empty:
#                         # keep last valid row (usually xx:00:01)
#                         cleaned_rows.append(non_zero_rows.sort_values("datetime").iloc[-1])
#                     else:
#                         # both zero → keep one
#                         cleaned_rows.append(zero_rows.iloc[0])

#                 hourly_df = pd.DataFrame(cleaned_rows).sort_values("datetime").reset_index(drop=True)

#                 print(f"Cleaned hourly data: {len(hourly_list)} → {len(hourly_df)} rows")
                                
                
#                 hourly_df["date"] = hourly_df["datetime"].dt.date

#                 # Determine sunrise/sunset per day
#                 sunrise_dict = {}
#                 sunset_dict = {}

#                 CITY_NAME = "Islamabad"
#                 LAT = 33.6844
#                 LON = 73.0479
#                 TIMEZONE = "Asia/Karachi"

#                 # Use API sunrise/sunset first
#                 if not weather_df.empty:
#                     for _, wr in weather_df.iterrows():
#                         d = wr.get('date')
#                         if pd.notnull(wr.get('sunrise')) and pd.notnull(wr.get('sunset')):
#                             sunrise_dict[d.date()] = wr.get('sunrise').time()
#                             sunset_dict[d.date()] = wr.get('sunset').time()

#                 # Fill missing using astral
#                 unique_days = hourly_df['date'].unique()

#                 for d in unique_days:
#                     if d not in sunrise_dict:
#                         if LocationInfo and sunrise and sunset:
#                             try:
#                                 loc = LocationInfo(
#                                     name=CITY_NAME,
#                                     region="Pakistan",
#                                     timezone=TIMEZONE,
#                                     latitude=LAT,
#                                     longitude=LON
#                                 )

#                                 sr = sunrise(loc.observer, date=d)
#                                 ss = sunset(loc.observer, date=d)

#                                 sunrise_dict[d] = sr.time()
#                                 sunset_dict[d] = ss.time()

#                             except Exception:
#                                 sunrise_dict[d] = dt_time(6, 0, 0)
#                                 sunset_dict[d] = dt_time(18, 0, 0)
#                         else:
#                             sunrise_dict[d] = dt_time(6, 0, 0)
#                             sunset_dict[d] = dt_time(18, 0, 0)

#                 # Map sunrise/sunset
#                 hourly_df['sunrise'] = hourly_df['date'].map(sunrise_dict)
#                 hourly_df['sunset'] = hourly_df['date'].map(sunset_dict)

#                 # Save hourly CSV
#                 hourly_out = os.path.join(out_dir, f"{sid}_dashboard-data-hourly.csv")

#                 hourly_df[
#                     ["datetime", "load", "pvProduced", "pvExported",
#                      "gridConsumed", "sunrise", "sunset"]
#                 ].to_csv(hourly_out, index=False)

#                 print(f"Hourly CSV saved: {hourly_out}")

#                 # -------------------------------
#                 # CORRECT IMPORT EXPORT CALCULATION
#                 # -------------------------------

#                 # Overall daily imports
#                 all_imports = hourly_df['gridConsumed'].dropna()

#                 overall_daily_avg = (
#                     float(all_imports.mean()) if not all_imports.empty else 0.0
#                 )

#                 overall_daily_peak = (
#                     float(all_imports.max()) if not all_imports.empty else 0.0
#                 )

#                 # Night calculation
#                 def is_night(row):
#                     sr = row['sunrise']
#                     ss = row['sunset']

#                     if pd.isna(sr) or pd.isna(ss):
#                         return False

#                     t = row['datetime'].time()
#                     return (t < sr) or (t > ss)


#                 hourly_df['is_night'] = hourly_df.apply(is_night, axis=1)

#                 night_imports = hourly_df.loc[
#                     hourly_df['is_night'], 'gridConsumed'
#                 ].dropna()

#                 overall_night_avg = (
#                     float(night_imports.mean()) if not night_imports.empty else 0.0
#                 )

#                 overall_night_peak = (
#                     float(night_imports.max()) if not night_imports.empty else 0.0
#                 )

#                 # Write CSV
#                 with open(import_export_out, 'w', newline='', encoding='utf-8') as f:
#                     writer = csv.DictWriter(
#                         f,
#                         fieldnames=[
#                             'system_id',
#                             'start',
#                             'end',
#                             'daily_avg_import',
#                             'daily_peak_import',
#                             'night_avg_import',
#                             'night_peak_import'
#                         ]
#                     )

#                     writer.writeheader()

#                     writer.writerow({
#                         'system_id': sid,
#                         'start': start_date,
#                         'end': end_date,
#                         'daily_avg_import': round(overall_daily_avg, 6),
#                         'daily_peak_import': round(overall_daily_peak, 6),
#                         'night_avg_import': round(overall_night_avg, 6),
#                         'night_peak_import': round(overall_night_peak, 6)
#                     })

#                 print(f"Import-export summary CSV saved: {import_export_out}")

#         except Exception as e:
#             print(f"Warning: hourly processing failed: {e}")

#     # # --- Hourly data processing ---
#     # hourly_out = None
#     # import_export_out = os.path.join(out_dir, "import-export.csv")
#     # if hourly_data is not None:
#     #     try:
#     #         hourly_list = hourly_data.get("systemHourlyEnergyStats", {}).get("values", [])
#     #         hourly_df = pd.DataFrame(hourly_list)
#     #         if not hourly_df.empty:
#     #             hourly_df["datetime"] = hourly_df["time"].apply(unix_to_datetime)
#     #             hourly_df["date"] = hourly_df["datetime"].dt.date

#     #             # Determine sunrise/sunset per day: prefer weather_df values, else use Astral fallback
#     #             sunrise_dict = {}
#     #             sunset_dict = {}
#     #             # prepare astral location if available
#     #             CITY_NAME = "Islamabad"
#     #             LAT = 33.6844
#     #             LON = 73.0479
#     #             TIMEZONE = "Asia/Karachi"
#     #             if not weather_df.empty:
#     #                 for _, wr in weather_df.iterrows():
#     #                     d = wr.get('date')
#     #                     if pd.notnull(wr.get('sunrise')) and pd.notnull(wr.get('sunset')):
#     #                         sunrise_dict[d.date()] = wr.get('sunrise').time()
#     #                         sunset_dict[d.date()] = wr.get('sunset').time()
#     #             # fill missing days using astral or defaults
#     #             unique_days = hourly_df['date'].unique()
#     #             for d in unique_days:
#     #                 if d not in sunrise_dict:
#     #                     if LocationInfo and sunrise and sunset:
#     #                         try:
#     #                             loc = LocationInfo(name=CITY_NAME, region="Pakistan", timezone=TIMEZONE, latitude=LAT, longitude=LON)
#     #                             sr = sunrise(loc.observer, date=d)
#     #                             ss = sunset(loc.observer, date=d)
#     #                             sunrise_dict[d] = sr.time()
#     #                             sunset_dict[d] = ss.time()
#     #                         except Exception as e:
#     #                             sunrise_dict[d] = dt_time(6, 0, 0)
#     #                             sunset_dict[d] = dt_time(18, 0, 0)
#     #                             print(f"Warning: Astral failed for {d}, using default 6AM/6PM: {e}")
#     #                     else:
#     #                         sunrise_dict[d] = dt_time(6, 0, 0)
#     #                         sunset_dict[d] = dt_time(18, 0, 0)

#     #             # map sunrise/sunset into hourly_df for convenience
#     #             hourly_df['sunrise'] = hourly_df['date'].map(sunrise_dict)
#     #             hourly_df['sunset'] = hourly_df['date'].map(sunset_dict)

#     #             # Save hourly CSV
#     #             hourly_out = os.path.join(out_dir, f"{sid}_dashboard-data-hourly.csv")
#     #             hourly_df[["datetime", "load", "pvProduced", "pvExported", "gridConsumed", "sunrise", "sunset"]].to_csv(hourly_out, index=False)
#     #             print(f"Hourly CSV saved: {hourly_out}")

#     #             # --- Build import-export.csv ---
#     #             # compute per-day stats first
#     #             per_day = []
#     #             for day, group in hourly_df.groupby('date'):
#     #                 g = group['gridConsumed'].dropna()
#     #                 if g.empty:
#     #                     continue
#     #                 daily_avg = float(g.mean())
#     #                 daily_peak = float(g.max())
#     #                 sr = sunrise_dict.get(day)
#     #                 ss = sunset_dict.get(day)
#     #                 if sr is None or ss is None:
#     #                     times = group['datetime'].dt.time
#     #                     night_mask = pd.Series([False]*len(group))
#     #                 else:
#     #                     times = group['datetime'].dt.time
#     #                     night_mask = (times < sr) | (times > ss)
#     #                 night_vals = group.loc[night_mask, 'gridConsumed'].dropna()
#     #                 night_avg = float(night_vals.mean()) if not night_vals.empty else 0.0
#     #                 night_peak = float(night_vals.max()) if not night_vals.empty else 0.0
#     #                 per_day.append({
#     #                     'daily_avg': daily_avg,
#     #                     'daily_peak': daily_peak,
#     #                     'night_avg': night_avg,
#     #                     'night_peak': night_peak
#     #                 })

#     #             # aggregate across days to single values
#     #             import math
#     #             if per_day:
#     #                 daily_avgs = [d['daily_avg'] for d in per_day]
#     #                 daily_peaks = [d['daily_peak'] for d in per_day]
#     #                 night_avgs = [d['night_avg'] for d in per_day]
#     #                 night_peaks = [d['night_peak'] for d in per_day]

#     #                 overall_daily_avg = sum(daily_avgs) / len(daily_avgs) if daily_avgs else 0.0
#     #                 overall_daily_peak = max(daily_peaks) if daily_peaks else 0.0
#     #                 overall_night_avg = sum(night_avgs) / len(night_avgs) if night_avgs else 0.0
#     #                 overall_night_peak = max(night_peaks) if night_peaks else 0.0
#     #             else:
#     #                 overall_daily_avg = overall_daily_peak = overall_night_avg = overall_night_peak = 0.0

#     #             # write single-row CSV
#     #             with open(import_export_out, 'w', newline='', encoding='utf-8') as f:
#     #                 writer = csv.DictWriter(f, fieldnames=['system_id','start','end','daily_avg_import','daily_peak_import','night_avg_import','night_peak_import'])
#     #                 writer.writeheader()
#     #                 writer.writerow({
#     #                     'system_id': sid,
#     #                     'start': start_date,
#     #                     'end': end_date,
#     #                     'daily_avg_import': round(overall_daily_avg, 6),
#     #                     'daily_peak_import': round(overall_daily_peak, 6),
#     #                     'night_avg_import': round(overall_night_avg, 6),
#     #                     'night_peak_import': round(overall_night_peak, 6)
#     #                 })
#     #             print(f"Import-export summary CSV saved: {import_export_out}")
#     #     except Exception as e:
#     #         print(f"Warning: hourly processing failed: {e}")

#     return {
#         'daily_csv': daily_out,
#         'monthly_csv': monthly_out,
#         'hourly_csv': hourly_out,
#         'import_export_csv': import_export_out if os.path.exists(import_export_out) else None
#     }


# if __name__ == '__main__':
#     # Load config
#     with open("conf.yaml", "r") as f:
#         config = yaml.safe_load(f)

#     defaults = config.get("defaults", {})

#     system_id = defaults.get("system_id")
#     start_date = defaults.get("start_date")
#     end_date = defaults.get("end_date")
#     out_dir = "."  # you can also add output directory to conf.yaml if needed

#     if not system_id:
#         raise ValueError("defaults.system_id is required in conf.yaml")

#     run_tosee_units(system_id, start_date, end_date, out_dir)
# # if __name__ == '__main__':
# #     import argparse
# #     parser = argparse.ArgumentParser()
# #     parser.add_argument('--system-id', required=False)
# #     parser.add_argument('--start', default='2025-02-01')
# #     parser.add_argument('--end', default='2026-02-28')
# #     parser.add_argument('--out-dir', default='.')
# #     args = parser.parse_args()
# #     run_tosee_units(args.system_id, args.start, args.end, args.out_dir)

import requests
import pandas as pd
from datetime import datetime, time as dt_time, timedelta
import os
import csv
import yaml
from pathlib import Path

# add astral for sunrise/sunset fallback
try:
    from astral import LocationInfo
    from astral.sun import sunrise, sunset
except Exception:
    LocationInfo = None
    sunrise = None
    sunset = None

# Load config from conf.yaml
CONFIG_PATH = Path(__file__).resolve().parent / "conf.yaml"
try:
    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f) or {}
    giles = config.get("giles", {})
    tosee_cfg = giles.get("toseeunits", {})
    API_URL = tosee_cfg.get("url") or os.getenv("GILES_API_URL")
    TOKEN = tosee_cfg.get("token") or os.getenv("GILES_API_TOKEN")
except Exception as e:
    print(f"Warning: could not load conf.yaml: {e}")

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

# --- GraphQL queries ---
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

def unix_to_datetime(ms):
    # Convert UTC -> Pakistan Time (+5 hours)
    return datetime.utcfromtimestamp(ms / 1000) + timedelta(hours=5)

def unix_to_datetime_utc(ms):
    # Return datetime from unix ms without applying timezone offset.
    # Use for sunrise/sunset which are already in Asia timezone.
    return datetime.utcfromtimestamp(ms / 1000)

def run_tosee_units(system_id, start_date, end_date, out_dir='.'):
    os.makedirs(out_dir, exist_ok=True)

    sid = system_id

    start_dt = datetime.fromisoformat(start_date)
    end_dt = datetime.fromisoformat(end_date)

    start_ts = int(start_dt.timestamp() * 1000)
    end_ts = int(end_dt.timestamp() * 1000)

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

    # --- Fetch Data ---
    weather_data = fetch_data(DAILY_WEATHER_QUERY, weather_variables)
    energy_data = fetch_data(DAILY_ENERGY_QUERY, energy_variables)
    hourly_data = fetch_data(HOURLY_ENERGY_QUERY, energy_variables)

    if weather_data is None or energy_data is None:
        raise SystemExit("Failed to fetch daily data from API.")

    # --- Process daily data ---
    weather_list = weather_data.get("dailyWeatherApp", [])
    energy_list = energy_data.get("systemDailyEnergyStats", {}).get("values", [])

    weather_df = pd.DataFrame(weather_list)
    energy_df = pd.DataFrame(energy_list)

    # Convert timestamps
    weather_df["date"] = weather_df["time"].apply(unix_to_datetime)
    if "sunrise" in weather_df.columns:
        # sunrise timestamps are already in Asia timezone — do not apply +5 offset
        weather_df["sunrise"] = weather_df["sunrise"].apply(unix_to_datetime_utc)
    if "sunset" in weather_df.columns:
        # sunset timestamps are already in Asia timezone — do not apply +5 offset
        weather_df["sunset"] = weather_df["sunset"].apply(unix_to_datetime_utc)
    weather_df["day"] = weather_df["date"].dt.date


    energy_df["date"] = energy_df["time"].apply(unix_to_datetime)
    energy_df["day"] = energy_df["date"].dt.date

    # Merge daily data on date
    daily_df = pd.merge(energy_df, weather_df, on="date", how="left")

    # Rename columns
    daily_df = daily_df.rename(columns={
        "pvProduced": "Solar Energy Produced",
        "pvExported": "Solar Exported",
        "gridConsumed": "Grid Consumed"
    })

    # --- Save daily CSV ---
    daily_out = os.path.join(out_dir, f"{sid}_dashboard-data-daily.csv")
    daily_df = daily_df[["date", "Solar Energy Produced", "Solar Exported", "Grid Consumed", "sunrise", "sunset"]]
    daily_df.to_csv(daily_out, index=False)
    print(f"Daily CSV saved: {daily_out}")

    # --- Monthly aggregation ---
    monthly_df = daily_df.copy()
    monthly_df["month"] = monthly_df["date"].dt.to_period("M")
    monthly_agg = monthly_df.groupby("month").agg({
        "Solar Energy Produced": "sum",
        "Grid Consumed": "sum",
        "Solar Exported": "sum"
    }).reset_index()
    monthly_agg["unitsT"] = monthly_agg["Grid Consumed"] - monthly_agg["Solar Exported"]

    monthly_out = os.path.join(out_dir, f"{sid}_dashboard-data-monthly.csv")
    monthly_agg.to_csv(monthly_out, index=False)
    print(f"Monthly CSV saved: {monthly_out}")

    # --- Hourly data processing ---
    hourly_out = None
    import_export_out = os.path.join(out_dir, "import-export.csv")

    if hourly_data is not None:
        try:
            hourly_list = hourly_data.get("systemHourlyEnergyStats", {}).get("values", [])
            hourly_df = pd.DataFrame(hourly_list)

            if not hourly_df.empty:
                hourly_df["datetime"] = hourly_df["time"].apply(unix_to_datetime)
                hourly_df["date"] = hourly_df["datetime"].dt.date

                # Map sunrise/sunset per day
                sunrise_dict = {}
                sunset_dict = {}
                CITY_NAME = "Islamabad"
                LAT = 33.6844
                LON = 73.0479
                TIMEZONE = "Asia/Karachi"

                if not weather_df.empty:
                    for _, wr in weather_df.iterrows():
                        d = wr.get('date')
                        if pd.notnull(wr.get('sunrise')) and pd.notnull(wr.get('sunset')):
                            sunrise_dict[d.date()] = wr.get('sunrise').time()
                            sunset_dict[d.date()] = wr.get('sunset').time()

                unique_days = hourly_df['date'].unique()
                for d in unique_days:
                    if d not in sunrise_dict:
                        if LocationInfo and sunrise and sunset:
                            try:
                                loc = LocationInfo(name=CITY_NAME, region="Pakistan", timezone=TIMEZONE, latitude=LAT, longitude=LON)
                                sr = sunrise(loc.observer, date=d)
                                ss = sunset(loc.observer, date=d)
                                sunrise_dict[d] = sr.time()
                                sunset_dict[d] = ss.time()
                            except Exception:
                                sunrise_dict[d] = dt_time(6,0,0)
                                sunset_dict[d] = dt_time(18,0,0)
                        else:
                            sunrise_dict[d] = dt_time(6, 0, 0)
                            sunset_dict[d] = dt_time(18, 0, 0)

                hourly_df['sunrise'] = hourly_df['date'].map(sunrise_dict)
                hourly_df['sunset'] = hourly_df['date'].map(sunset_dict)

                # --- Clean bad duplicate hourly rows ---
                hourly_df["hour"] = hourly_df["datetime"].dt.floor("H")
                def is_all_zero(row):
                    return (row.get("load",0)==0 and row.get("pvProduced",0)==0 and
                            row.get("pvExported",0)==0 and row.get("gridConsumed",0)==0)

                cleaned_rows = []
                for hour, group in hourly_df.groupby("hour"):
                    if len(group)==1:
                        cleaned_rows.append(group.iloc[0])
                        continue
                    zero_rows = group[group.apply(is_all_zero, axis=1)]
                    non_zero_rows = group[~group.apply(is_all_zero, axis=1)]
                    if not non_zero_rows.empty:
                        cleaned_rows.append(non_zero_rows.sort_values("datetime").iloc[-1])
                    else:
                        cleaned_rows.append(zero_rows.iloc[0])

                hourly_df = pd.DataFrame(cleaned_rows).sort_values("datetime").reset_index(drop=True)

                # --- Save hourly CSV ---
                hourly_out = os.path.join(out_dir, f"{sid}_dashboard-data-hourly.csv")
                hourly_df[["datetime","load","pvProduced","pvExported","gridConsumed","sunrise","sunset"]].to_csv(hourly_out, index=False)
                print(f"Hourly CSV saved: {hourly_out}")

                # --- Calculate import-export stats ---
                # Daily avg/peak from daily CSV
                daily_df_for_stats = pd.read_csv(daily_out)
                daily_grid = daily_df_for_stats["Grid Consumed"].dropna()
                overall_daily_avg = float(daily_grid.mean()) if not daily_grid.empty else 0.0
                overall_daily_peak = float(daily_grid.max()) if not daily_grid.empty else 0.0

                # Day/Night avg & peak from hourly
                def is_night(row):
                    sr = row['sunrise']
                    ss = row['sunset']
                    if pd.isna(sr) or pd.isna(ss):
                        return False
                    t = row['datetime'].time()
                    return (t < sr) or (t > ss)
                hourly_df['is_night'] = hourly_df.apply(is_night, axis=1)

                night_imports = hourly_df.loc[hourly_df['is_night'],'gridConsumed'].dropna()
                overall_night_avg = float(night_imports.mean()) if not night_imports.empty else 0.0
                overall_night_peak = float(night_imports.max()) if not night_imports.empty else 0.0

                day_imports = hourly_df.loc[~hourly_df['is_night'],'gridConsumed'].dropna()
                overall_day_avg = float(day_imports.mean()) if not day_imports.empty else 0.0
                overall_day_peak = float(day_imports.max()) if not day_imports.empty else 0.0

                # Write import-export summary CSV
                with open(import_export_out, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(
                        f,
                        fieldnames=['system_id','start','end',
                                    'daily_avg_import','daily_peak_import',
                                    'day_avg_import','day_peak_import',
                                    'night_avg_import','night_peak_import']
                    )
                    writer.writeheader()
                    writer.writerow({
                        'system_id': sid,
                        'start': start_date,
                        'end': end_date,
                        'daily_avg_import': round(overall_daily_avg,6),
                        'daily_peak_import': round(overall_daily_peak,6),
                        'day_avg_import': round(overall_day_avg,6),
                        'day_peak_import': round(overall_day_peak,6),
                        'night_avg_import': round(overall_night_avg,6),
                        'night_peak_import': round(overall_night_peak,6)
                    })
                print(f"Import-export summary CSV saved: {import_export_out}")

        except Exception as e:
            print(f"Warning: hourly processing failed: {e}")

    return {
        'daily_csv': daily_out,
        'monthly_csv': monthly_out,
        'hourly_csv': hourly_out,
        'import_export_csv': import_export_out if os.path.exists(import_export_out) else None
    }

# -------------------
# MAIN
# -------------------
if __name__ == '__main__':
    with open("conf.yaml","r") as f:
        config = yaml.safe_load(f)

    defaults = config.get("defaults",{})
    system_id = defaults.get("system_id")
    start_date = defaults.get("start_date")
    end_date = defaults.get("end_date")
    out_dir = "."

    if not system_id:
        raise ValueError("defaults.system_id is required in conf.yaml")

    run_tosee_units(system_id, start_date, end_date, out_dir)