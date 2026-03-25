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


current = start_date

print("Fetching hourly grid data...")

while current < end_date:

    next_day = current + timedelta(days=1)

    start_ts = int(current.timestamp() * 1000)
    end_ts = int(next_day.timestamp() * 1000)

    values = get_grid_stats(system_id, start_ts, end_ts)

    if values:
        all_data.extend(values)

    print("Fetched:", current.date())

    current = next_day


# Convert to dataframe
df = pd.DataFrame(all_data)

df["datetime"] = pd.to_datetime(df["time"], unit="ms")

df = df.rename(columns={"power": "grid_power"})

df = df.sort_values("datetime")


# Save raw CSV
df.to_csv("grid_hourly_2025.csv", index=False)

print("Saved hourly CSV")


# Peak value
peak_power = df["grid_power"].max()


# 365 day average
df["date"] = df["datetime"].dt.date
daily_avg = df.groupby("date")["grid_power"].mean()

avg_365 = daily_avg.tail(365).mean()


summary = pd.DataFrame([{
    "system_id": system_id,
    "peak_import_power": peak_power,
    "avg_365_days": avg_365
}])

summary.to_csv("grid_summary.csv", index=False)

print("Saved summary CSV")