# #!/usr/bin/env python3
# import argparse
# import csv
# import json
# import sys
# from datetime import datetime
# import math

# try:
#     from cassandra.cluster import Cluster
#     from cassandra.auth import PlainTextAuthProvider
#     from cassandra.query import SimpleStatement
#     from cassandra.cluster import NoHostAvailable
# except Exception:
#     print("Error: cassandra-driver is required. Install with: pip install cassandra-driver")
#     sys.exit(1)

# import yaml

# # --- Config ---
# def read_config():
#     try:
#         with open("conf.yaml", "r") as f:
#             cfg = yaml.safe_load(f)
#     except Exception:
#         cfg = {}
#     sc = cfg.get("scylla", {})
#     host = sc.get("host", "127.0.0.1")
#     if host == "localhost":
#         host = "127.0.0.1"
#     return {
#         "hosts": [host] if isinstance(host, str) else host,
#         "port": sc.get("port", 5533),
#         "username": sc.get("username"),
#         "password": sc.get("password"),
#         "keyspace": sc.get("keyspace"),
#         "try_for_times": int(sc.get("try_for_times", 5)),
#     }

# # --- Connect to Scylla ---
# def connect_scylla(conf):
#     auth = None
#     if conf["username"] and conf["password"]:
#         auth = PlainTextAuthProvider(username=conf["username"], password=conf["password"]) 

#     cluster, session, last_exc = None, None, None
#     for attempt in range(1, conf["try_for_times"] + 1):
#         try:
#             cluster = Cluster(contact_points=conf["hosts"], port=conf["port"], auth_provider=auth, connect_timeout=30)
#             session = cluster.connect(keyspace=conf.get("keyspace"))
#             return cluster, session
#         except NoHostAvailable as e:
#             last_exc = e
#             print(f"NoHostAvailable (attempt {attempt}/{conf['try_for_times']}): {getattr(e,'errors', None)}")
#         except Exception as e:
#             last_exc = e
#             print(f"Connection failed (attempt {attempt}/{conf['try_for_times']}): {e}")
#     raise RuntimeError(f"Failed to connect to Scylla: {last_exc}")

# # --- Query BMS SOC daily ---
# def query_bms_soc(session, system_id, start_date):
#     cql = f"""
# SELECT day, sum, count
# FROM bms_soc_1d
# WHERE system_id = {system_id} AND day > '{start_date}';
# """
#     stmt = SimpleStatement(cql)
#     rows = session.execute(stmt)
#     results = []
#     for r in rows:
#         try:
#             row = r._asdict()
#         except Exception:
#             try:
#                 row = dict(r)
#             except Exception:
#                 row = r
#         s, c = row.get("sum"), row.get("count")
#         if s is None or c in (None, 0):
#             continue
#         val = float(s) / float(c)
#         day = row.get("day")
#         if isinstance(day, str):
#             try:
#                 day = datetime.strptime(day, "%Y-%m-%d").date()
#             except Exception:
#                 day = None
#         results.append({"day": day, "bms_soc": val})
#     return results

# # --- Analyze daily, monthly, yearly averages ---
# def analyze_and_write(system_id, rows, out_csv):
#     if not rows:
#         print("No data returned from Scylla for given system_id and date range.")
#         return

#     # Organize by day, month, year
#     daily_vals = {}
#     monthly_vals = {}
#     yearly_vals = {}

#     for r in rows:
#         day = r["day"]
#         if not day:
#             continue
#         val = r["bms_soc"]
#         daily_vals.setdefault(day, []).append(val)
#         ym = f"{day.year}-{day.month:02d}"
#         monthly_vals.setdefault(ym, []).append(val)
#         yearly_vals.setdefault(day.year, []).append(val)

#     # Compute daily averages per day
#     daily_avg = {d: sum(vals)/len(vals) for d, vals in daily_vals.items() if vals}
#     monthly_avg = {m: sum(vals)/len(vals) for m, vals in monthly_vals.items() if vals}
#     yearly_avg = {y: sum(vals)/len(vals) for y, vals in yearly_vals.items() if vals}

#     # Lowest value across all days, months, years
#     lowest_daily_avg_bms_soc = min(daily_avg.values()) if daily_avg else None
#     lowest_monthly_avg_bms_soc = min(monthly_avg.values()) if monthly_avg else None
#     lowest_yearly_avg_bms_soc = min(yearly_avg.values()) if yearly_avg else None

#     # Write CSV summary
#     fieldnames = [
#         "system_id",
#         "start_date",
#         "end_date",
#         "lowest_daily_avg_bms_soc",
#         "lowest_monthly_avg_bms_soc",
#         "lowest_yearly_avg_bms_soc"
#     ]
#     start = min(r["day"] for r in rows if r["day"])
#     end = max(r["day"] for r in rows if r["day"])
#     row = {
#         "system_id": system_id,
#         "start_date": start.isoformat(),
#         "end_date": end.isoformat(),
#         "lowest_daily_avg_bms_soc": round(lowest_daily_avg_bms_soc, 3) if lowest_daily_avg_bms_soc is not None else None,
#         "lowest_monthly_avg_bms_soc": round(lowest_monthly_avg_bms_soc, 3) if lowest_monthly_avg_bms_soc is not None else None,
#         "lowest_yearly_avg_bms_soc": round(lowest_yearly_avg_bms_soc, 3) if lowest_yearly_avg_bms_soc is not None else None
#     }

#     with open(out_csv, "w", newline="", encoding="utf-8") as f:
#         writer = csv.DictWriter(f, fieldnames=fieldnames)
#         writer.writeheader()
#         writer.writerow(row)

#     print(f"Wrote BMS SOC summary to {out_csv}")

# # --- Main ---
# def main():
#     parser = argparse.ArgumentParser()
#     parser.add_argument("--system-id", required=True)
#     parser.add_argument("--start", default="2024-01-01")
#     parser.add_argument("--out", default="bms_soc_summary.csv")
#     args = parser.parse_args()

#     conf = read_config()
#     cluster, session = None, None
#     try:
#         cluster, session = connect_scylla(conf)
#         rows = query_bms_soc(session, args.system_id, args.start)
#         analyze_and_write(args.system_id, rows, args.out)
#     finally:
#         if session:
#             session.shutdown()
#         if cluster:
#             cluster.shutdown()

# if __name__ == "__main__":
#     main()







#!/usr/bin/env python3
import csv
import requests
import yaml
from datetime import datetime
from collections import defaultdict


# --- Config ---
def read_config():
    try:
        with open("conf.yaml", "r") as f:
            cfg = yaml.safe_load(f)
    except Exception:
        cfg = {}

    defaults = cfg.get("defaults", {})
    api = cfg.get("giles", {}).get("apiwork", {})

    return {
        "system_id": defaults.get("system_id"),
        "start_date": defaults.get("start_date"),
        "end_date": defaults.get("end_date"),
        "url": api.get("url"),
        "token": api.get("token")
    }


# --- Fetch Battery SOC from API ---
def fetch_bms_soc(conf):

    start_ts = int(datetime.fromisoformat(conf["start_date"]).timestamp() * 1000)
    end_ts = int(datetime.fromisoformat(conf["end_date"]).timestamp() * 1000)

    headers = {
        "Authorization": f"Bearer {conf['token']}",
        "Content-Type": "application/json"
    }

    payload = {
        "operationName": "getStatsQuery",
        "variables": {
            "systemId": conf["system_id"],
            "startTimeStamp": start_ts,
            "endTimeStamp": end_ts
        },
        "query": """
query getStatsQuery($endTimeStamp: Int!, $startTimeStamp: Int!, $systemId: String) {
  BatteryStatsApp(
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

    response = requests.post(conf["url"], json=payload, headers=headers)

    if response.status_code != 200:
        raise RuntimeError(f"API Error {response.status_code}: {response.text}")

    data = response.json()

    values = data.get("data", {}).get("BatteryStatsApp", {}).get("values", [])

    results = []

    for v in values:
        ts = v.get("time")
        power = v.get("power")

        if ts is None or power is None:
            continue

        day = datetime.fromtimestamp(ts / 1000).date()

        results.append({
            "day": day,
            "bms_soc": float(power)
        })

    return results


# --- Analyze daily, monthly, yearly averages ---
def analyze_and_write(system_id, rows, out_csv):

    if not rows:
        print("No data returned from API")
        return

    daily_vals = defaultdict(list)
    monthly_vals = defaultdict(list)
    yearly_vals = defaultdict(list)

    for r in rows:
        day = r["day"]
        val = r["bms_soc"]

        daily_vals[day].append(val)

        ym = f"{day.year}-{day.month:02d}"
        monthly_vals[ym].append(val)

        yearly_vals[day.year].append(val)

    daily_avg = {d: sum(v)/len(v) for d, v in daily_vals.items()}
    monthly_avg = {m: sum(v)/len(v) for m, v in monthly_vals.items()}
    yearly_avg = {y: sum(v)/len(v) for y, v in yearly_vals.items()}

    lowest_daily_avg_bms_soc = min(daily_avg.values()) if daily_avg else None
    lowest_monthly_avg_bms_soc = min(monthly_avg.values()) if monthly_avg else None
    lowest_yearly_avg_bms_soc = min(yearly_avg.values()) if yearly_avg else None

    start = min(daily_avg.keys())
    end = max(daily_avg.keys())

    row = {
        "system_id": system_id,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "lowest_daily_avg_bms_soc": round(lowest_daily_avg_bms_soc, 3) if lowest_daily_avg_bms_soc else None,
        "lowest_monthly_avg_bms_soc": round(lowest_monthly_avg_bms_soc, 3) if lowest_monthly_avg_bms_soc else None,
        "lowest_yearly_avg_bms_soc": round(lowest_yearly_avg_bms_soc, 3) if lowest_yearly_avg_bms_soc else None
    }

    fieldnames = list(row.keys())

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(row)

    print(f"Wrote BMS SOC summary to {out_csv}")


# --- Main ---
def main():

    conf = read_config()

    rows = fetch_bms_soc(conf)

    analyze_and_write(
        conf["system_id"],
        rows,
        "bms_soc_summary.csv"
    )


if __name__ == "__main__":
    main()