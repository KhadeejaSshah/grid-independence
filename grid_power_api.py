# import requests
# import yaml
# import pandas as pd
# from pathlib import Path
# import os
# from datetime import datetime, timezone

# # ---------------- LOAD CONFIG ----------------
# CONFIG_PATH = Path(__file__).resolve().parent / "conf.yaml"

# try:
#     with open(CONFIG_PATH, "r") as f:
#         config = yaml.safe_load(f) or {}
#     giles = config.get("giles", {})
#     apiwork_cfg = giles.get("apiwork", {})
#     API_URL = apiwork_cfg.get("url") or os.getenv("GILES_API_URL")
#     TOKEN = apiwork_cfg.get("token") or os.getenv("GILES_API_TOKEN")
# except Exception as e:
#     print(f"Warning: could not load conf.yaml: {e}")


# # ---------------- FETCH GRID DATA ----------------
# def get_grid_stats(system_id, start_ts, end_ts):

#     headers = {
#         "Authorization": f"Bearer {TOKEN}",
#         "Content-Type": "application/json"
#     }

#     payload = {
#         "operationName": "getStatsQuery",
#         "variables": {
#             "systemId": system_id,
#             "startTimeStamp": start_ts,
#             "endTimeStamp": end_ts
#         },
#         "query": """
#         query getStatsQuery($endTimeStamp: Int!, $startTimeStamp: Int!, $systemId: String) {
#           GridStatsApp(
#             startTimestamp: $startTimeStamp
#             endTimestamp: $endTimeStamp
#             systemId: $systemId
#           ) {
#             interval
#             values {
#               power
#               time
#             }
#           }
#         }
#         """
#     }

#     response = requests.post(API_URL, json=payload, headers=headers)

#     if response.status_code != 200:
#         print("API Error:", response.text)
#         return None

#     data = response.json()

#     if "errors" in data:
#         print("API returned errors:", data["errors"])
#         return None

#     return data


# # ---------------- MAIN ----------------
# if __name__ == "__main__":

#     system_id = "7ff25f2d-93a8-49e9-aea7-d258c1e82155"

#     # 2025 Jan 1 → Now
#     start_dt = datetime(2025, 1, 1, tzinfo=timezone.utc)
#     end_dt = datetime.now(timezone.utc)

#     start_ts = int(start_dt.timestamp() * 1000)
#     end_ts = int(end_dt.timestamp() * 1000)

#     print("Fetching Grid data from 2025 → now...")

#     data = get_grid_stats(system_id, start_ts, end_ts)

#     if not data:
#         print("No data fetched")
#         exit()

#     values = data["data"]["GridStatsApp"]["values"]

#     # ---------------- RAW CSV ----------------
#     df = pd.DataFrame(values)

#     df["datetime"] = pd.to_datetime(df["time"], unit="ms")
#     df = df.rename(columns={"power": "grid_power"})

#     raw_csv = "grid_raw_2025.csv"
#     df.to_csv(raw_csv, index=False)

#     print(f"Raw CSV saved: {raw_csv}")


#     # ---------------- SUMMARY CSV ----------------

#     # Peak import power (max of all time)
#     peak_power = df["grid_power"].max()

#     # 365 day average
#     df["date"] = df["datetime"].dt.date
#     daily_avg = df.groupby("date")["grid_power"].mean()

#     avg_365 = daily_avg.tail(365).mean()

#     summary_df = pd.DataFrame([{
#         "system_id": system_id,
#         "peak_import_power": peak_power,
#         "avg_365_days": avg_365
#     }])

#     summary_csv = "grid_summary.csv"
#     summary_df.to_csv(summary_csv, index=False)

#     print(f"Summary CSV saved: {summary_csv}")

#     print("\nDone ✅")

import requests
import yaml
import pandas as pd
from pathlib import Path
import os
from datetime import datetime, timedelta, timezone


CONFIG_PATH = Path(__file__).resolve().parent / "conf.yaml"

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

API_URL = config["giles"]["apiwork"]["url"]
TOKEN = config["giles"]["apiwork"]["token"]


def get_grid_stats(system_id, start_ts, end_ts):

    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "operationName": "getStatsQuery",
        "variables": {
            "systemId": system_id,
            "startTimeStamp": start_ts,
            "endTimeStamp": end_ts
        },
        "query": """
        query getStatsQuery($endTimeStamp: Int!, $startTimeStamp: Int!, $systemId: String) {
          GridStatsApp(
            startTimestamp: $startTimeStamp
            endTimestamp: $endTimeStamp
            systemId: $systemId
          ) {
            interval
            values {
              power
              time
            }
          }
        }
        """
    }

    response = requests.post(API_URL, json=payload, headers=headers)

    if response.status_code != 200:
        return None

    data = response.json()

    if "errors" in data:
        return None

    return data["data"]["GridStatsApp"]["values"]


# -------- MAIN --------

# system_id = "7ff25f2d-93a8-49e9-aea7-d258c1e82155"

# start_date = datetime(2026, 1, 1, tzinfo=timezone.utc)
# end_date = datetime.now(timezone.utc)
with open("conf.yaml", "r") as f:
    config = yaml.safe_load(f)

defaults = config.get("defaults", {})

system_id = defaults.get("system_id")
start_date = defaults.get("start_date")
end_date = defaults.get("end_date")

# Ensure start_date/end_date are datetime objects (expect ISO strings in conf)
all_data = []
try:
    if isinstance(start_date, str):
        # try parse ISO format
        try:
            start_dt = datetime.fromisoformat(start_date)
            if start_dt.tzinfo is None:
                start_dt = start_dt.replace(tzinfo=timezone.utc)
        except Exception:
            # fallback: parse date only
            start_dt = datetime.fromisoformat(start_date + 'T00:00:00')
            start_dt = start_dt.replace(tzinfo=timezone.utc)
    elif isinstance(start_date, datetime):
        start_dt = start_date
    else:
        # default fallback
        start_dt = datetime.now(timezone.utc) - timedelta(days=365)

    if isinstance(end_date, str):
        try:
            end_dt = datetime.fromisoformat(end_date)
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
        except Exception:
            end_dt = datetime.fromisoformat(end_date + 'T00:00:00')
            end_dt = end_dt.replace(tzinfo=timezone.utc)
    elif isinstance(end_date, datetime):
        end_dt = end_date
    else:
        end_dt = datetime.now(timezone.utc)
except Exception:
    start_dt = datetime.now(timezone.utc) - timedelta(days=365)
    end_dt = datetime.now(timezone.utc)

current = start_dt

print("Fetching hourly grid data...")

# iterate day-by-day (or in chunks) and collect values
while current < end_dt:

    next_day = current + timedelta(days=1)

    start_ts = int(current.timestamp() * 1000)
    end_ts = int(next_day.timestamp() * 1000)

    values = get_grid_stats(system_id, start_ts, end_ts)

    if values:
        all_data.extend(values)

    print("Fetched:", current.date())

    current = next_day


# Convert to dataframe
if not all_data:
    print("No grid data fetched")
    df = pd.DataFrame(columns=["time", "power"])
else:
    df = pd.DataFrame(all_data)

df["datetime"] = pd.to_datetime(df["time"], unit="ms")

df = df.rename(columns={"power": "grid_power"})

df = df.sort_values("datetime")


# Save raw CSV
raw_csv = "grid_hourly_2025.csv"
df.to_csv(raw_csv, index=False)
print(f"Saved hourly CSV: {raw_csv}")

# If dataframe empty, create zero-summary
if df.empty or 'grid_power' not in df.columns:
    print("No grid power data available to compute metrics.")
    summary_df = pd.DataFrame([{
        "system_id": system_id,
        "avg_daily_import_kwh": None,
        "avg_day_import_kwh": None,
        "avg_night_import_kwh": None,
        "peak_daily_import_kwh": None,
        "peak_day_import_kwh": None,
        "peak_night_import_kwh": None,
        "peak_import_power_kw": None,
        "avg_365_days": None
    }])
    summary_csv = "grid_summary.csv"
    summary_df.to_csv(summary_csv, index=False)
    print(f"Saved summary CSV: {summary_csv}")
else:
    # Ensure sorted and datetime present
    df = df.sort_values('datetime').reset_index(drop=True)
    # convert instantaneous power to kW
    df['grid_power_kw'] = pd.to_numeric(df['grid_power'], errors='coerce') / 1000.0

    # compute interval to next sample in seconds
    df['dt_next'] = df['datetime'].shift(-1)
    df['delta_s'] = (df['dt_next'] - df['datetime']).dt.total_seconds()
    # fill last delta with median delta or 3600s
    median_delta = int(df['delta_s'].median(skipna=True) or 3600)
    df['delta_s'] = df['delta_s'].fillna(median_delta)
    df.loc[df['delta_s'] <= 0, 'delta_s'] = median_delta

    # energy in kWh for the interval following each timestamp
    df['energy_kwh'] = df['grid_power_kw'] * (df['delta_s'] / 3600.0)

    # compute midpoint for each interval
    mid_secs = (df['delta_s'] / 2).fillna(0)
    df['midpoint'] = df['datetime'] + pd.to_timedelta(mid_secs, unit='s')

    # --- Determine day/night using Astral sunrise/sunset per day ---
    # use Astral for accurate sunrise/sunset times; fallback to 06:00/18:00
    try:
        from astral import LocationInfo
        from astral.sun import sunrise, sunset
        from datetime import time as dt_time

        # default location values (change if desired)
        CITY_NAME = 'Islamabad'
        LAT = 33.6844
        LON = 73.0479
        TIMEZONE = 'Asia/Karachi'

        location = LocationInfo(name=CITY_NAME, region='Pakistan', timezone=TIMEZONE, latitude=LAT, longitude=LON)

        # build sunrise/sunset lookup for all dates present (based on midpoint date)
        sunrise_dict = {}
        sunset_dict = {}
        for d in pd.to_datetime(df['midpoint'].dt.date.unique()):
            day = d.date()
            try:
                sr = sunrise(location.observer, date=day)
                ss = sunset(location.observer, date=day)
                sunrise_dict[day] = sr.time()
                sunset_dict[day] = ss.time()
            except Exception:
                sunrise_dict[day] = dt_time(6, 0, 0)
                sunset_dict[day] = dt_time(18, 0, 0)

        def is_day_midpoint(row):
            mp = row['midpoint']
            day = mp.date()
            t = mp.time()
            sr = sunrise_dict.get(day)
            ss = sunset_dict.get(day)
            if sr is None or ss is None:
                return 6 <= t.hour < 18
            return (t >= sr and t <= ss)

        df['is_day'] = df.apply(is_day_midpoint, axis=1)
    except Exception:
        # fallback simple heuristic: 06:00-17:59
        df['is_day'] = df['midpoint'].dt.hour.between(6, 17)

    # assign local date for grouping (date of the interval midpoint)
    df['day_date'] = df['midpoint'].dt.date

    # compute daily totals
    daily_total = df.groupby('day_date')['energy_kwh'].sum()
    daily_day = df[df['is_day']].groupby('day_date')['energy_kwh'].sum()
    daily_night = df[~df['is_day']].groupby('day_date')['energy_kwh'].sum()

    # compute requested metrics (use float or None)
    def positive_mean(s):
        try:
            ser = pd.to_numeric(s, errors='coerce').dropna()
            ser_pos = ser[ser > 0]
            if ser_pos.empty:
                return None
            return float(ser_pos.mean())
        except Exception:
            return None

    def positive_max(s):
        try:
            ser = pd.to_numeric(s, errors='coerce').dropna()
            ser_pos = ser[ser > 0]
            if ser_pos.empty:
                return None
            return float(ser_pos.max())
        except Exception:
            return None

    avg_daily_import_kwh = positive_mean(daily_total)
    avg_day_import_kwh = positive_mean(daily_day)
    avg_night_import_kwh = positive_mean(daily_night)
    peak_daily_import_kwh = positive_max(daily_total)
    peak_day_import_kwh = positive_max(daily_day)
    peak_night_import_kwh = positive_max(daily_night)

    # peak instantaneous import power in kW (only positive imports)
    peak_import_power_kw = positive_max(df['grid_power_kw'])

    # keep the existing avg_365 computation if available
    avg_365_val = None
    try:
        # compute daily mean of grid_power (kW) and then average last 365 days
        df['date'] = df['datetime'].dt.date
        daily_mean_power = df.groupby('date')['grid_power_kw'].mean()
        avg_365_val = positive_mean(daily_mean_power.tail(365)) if not daily_mean_power.empty else None
    except Exception:
        avg_365_val = None

    summary = {
        "system_id": system_id,
        "avg_daily_import_kwh_grid": avg_daily_import_kwh,
        "avg_day_import_kwh_grid": avg_day_import_kwh,
        "avg_night_import_kwh_grid": avg_night_import_kwh,
        "peak_daily_import_kwh_grid": peak_daily_import_kwh,
        "peak_day_import_kwh_grid": peak_day_import_kwh,
        "peak_night_import_kwh_grid": peak_night_import_kwh,
        "peak_import_power_kw_grid": peak_import_power_kw,
        "avg_365_days_grid": avg_365_val
    }

    summary_df = pd.DataFrame([summary])
    summary_csv = "grid_summary.csv"
    summary_df.to_csv(summary_csv, index=False)
    print(f"Saved summary CSV: {summary_csv}")

print("\nDone ✅")