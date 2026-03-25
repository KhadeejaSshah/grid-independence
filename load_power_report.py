import csv
import json
from datetime import datetime
import requests
import yaml
from pathlib import Path
import os

# -------- CONFIG --------
CONFIG_PATH = Path(__file__).resolve().parent / "conf.yaml"
try:
    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f) or {}
    giles = config.get("giles", {})
    apiwork_cfg = giles.get("apiwork", {})
    API_URL = apiwork_cfg.get("url") or os.getenv("GILES_API_URL")
    TOKEN = apiwork_cfg.get("token") or os.getenv("GILES_API_TOKEN")
    with open("conf.yaml", "r") as f:
        config = yaml.safe_load(f)

    defaults = config.get("defaults", {})

    SYSTEM_ID = defaults.get("system_id")
    START_DATE = defaults.get("start_date")
    END_DATE = defaults.get("end_date")
except Exception as e:
    print(f"Warning: could not load conf.yaml: {e}")
    API_URL = os.getenv("GILES_API_URL")
    TOKEN = os.getenv("GILES_API_TOKEN")
    SYSTEM_ID = None
    START_DATE = "2025-01-01"
    END_DATE = None


# -------- FETCH LOAD DATA FROM API --------
def fetch_load_data(system_id, start_ts, end_ts):
    headers = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
    payload = {
        "operationName": "getStatsQuery",
        "variables": {
            "systemId": system_id,
            "startTimeStamp": start_ts,
            "endTimeStamp": end_ts
        },
        "query": """
query getStatsQuery($endTimeStamp: Int!, $startTimeStamp: Int!, $systemId: String) {
  LoadStatsApp(
    startTimestamp: $startTimeStamp
    endTimestamp: $endTimeStamp
    systemId: $systemId
  ) {
    interval
    values {
      power
      time
      __typename
    }
    __typename
  }
}
"""
    }
    resp = requests.post(API_URL, json=payload, headers=headers)
    if resp.status_code != 200:
        raise RuntimeError(f"API Error {resp.status_code}: {resp.text}")
    data = resp.json()
    values = data.get("data", {}).get("LoadStatsApp", {}).get("values", [])
    results = []
    for v in values:
        ts = v.get("time")
        power = v.get("power")
        if ts and power is not None:
            dt = datetime.fromtimestamp(ts / 1000.0)
            results.append({"day": dt.date(), "value": float(power)})
    return results


# -------- ANALYZE AND WRITE CSV --------
def analyze_and_write(system_id, rows, out_csv):
    if not rows:
        print("No load data returned for given system_id and date range.")
        return

    yearly = {}
    monthly = {}
    summer_months = {6, 7, 8}
    winter_months = {12, 1, 2}

    for r in rows:
        day = r["day"]
        val = r["value"]
        year = day.year
        ym = f"{day.year}-{day.month:02d}"
        yearly.setdefault(year, []).append(val)
        monthly.setdefault(ym, []).append(val)

    yearly_peaks = {y: max(vals) for y, vals in yearly.items() if vals}
    monthly_peaks = {m: max(vals) for m, vals in monthly.items() if vals}

    summer_vals = [v for r in rows if r["day"].month in summer_months for v in [r["value"]]]
    winter_vals = [v for r in rows if r["day"].month in winter_months for v in [r["value"]]]

    summer_peak = max(summer_vals) if summer_vals else None
    winter_peak = max(winter_vals) if winter_vals else None

    sorted_years = sorted(yearly.keys())
    growth_factor = None
    if len(sorted_years) >= 2:
        first_avg = sum(yearly[sorted_years[0]]) / len(yearly[sorted_years[0]])
        last_avg = sum(yearly[sorted_years[-1]]) / len(yearly[sorted_years[-1]])
        if first_avg != 0:
            growth_factor = last_avg / first_avg

    overall_yearly_peak = max(yearly_peaks.values()) if yearly_peaks else None
    overall_monthly_peak = max(monthly_peaks.values()) if monthly_peaks else None

    start = min(r["day"] for r in rows)
    end = max(r["day"] for r in rows)

    row = {
        "system_id": system_id,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "overall_yearly_peak": overall_yearly_peak,
        "overall_monthly_peak": overall_monthly_peak,
        "summer_peak": summer_peak,
        "winter_peak": winter_peak,
        "growth_factor": round(growth_factor, 3) if growth_factor else None,
        "yearly_peaks": json.dumps(yearly_peaks),
        "monthly_peaks": json.dumps(monthly_peaks),
    }

    fieldnames = list(row.keys())
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(row)
    print(f"Wrote summary to {out_csv}")

    # peak + 365-day avg CSV
    peak_val = max(r["value"] for r in rows)
    avg_val = sum(r["value"] for r in rows) / len(rows)
    with open("load_peak_avg.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["peak_power", "avg_power"])
        writer.writeheader()
        writer.writerow({"peak_power": peak_val, "avg_power": avg_val})
    print("Wrote peak+average CSV to load_peak_avg.csv")


# -------- MAIN --------
def main():
    if not SYSTEM_ID:
        print("Error: system_id not set in conf.yaml")
        return

    start_date = datetime.fromisoformat(START_DATE)
    end_date = datetime.fromisoformat(END_DATE) if END_DATE else datetime.today()

    start_ts = int(start_date.timestamp() * 1000)
    end_ts = int(end_date.timestamp() * 1000)

    rows = fetch_load_data(SYSTEM_ID, start_ts, end_ts)
    analyze_and_write(SYSTEM_ID, rows, "load-power.csv")


if __name__ == "__main__":
    main()