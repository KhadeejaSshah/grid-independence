import os
import pandas as pd
from datetime import datetime


def run_compile_raw(raw_path, system_id, start_date, end_date, out_dir='.'):
    os.makedirs(out_dir, exist_ok=True)
    if not os.path.exists(raw_path):
        print(f"Warning: raw file {raw_path} not found. Skipping compile.")
        return None

    df = pd.read_csv(raw_path, parse_dates=["datetime"]) 
    # Convert sunrise/sunset to datetime where possible
    try:
        df["sunrise"] = pd.to_datetime(df["sunrise"]).dt.time
        df["sunset"] = pd.to_datetime(df["sunset"]).dt.time
    except Exception:
        pass

    def time_diff_hours(start, end):
        return (datetime.combine(datetime.min, end) - datetime.combine(datetime.min, start)).total_seconds() / 3600

    df["day"] = df["datetime"].dt.date
    df["sun_hours"] = df.apply(lambda row: time_diff_hours(row["sunrise"], row["sunset"]) if pd.notnull(row["sunrise"]) and pd.notnull(row["sunset"]) else None, axis=1)

    # Average sun hours per day
    sun_hours_daily = df.groupby("day")["sun_hours"].mean().reset_index()
    sun_hours_daily = sun_hours_daily.rename(columns={"sun_hours": "sun_hours_per_day"})

    avg_sun_hours = sun_hours_daily["sun_hours_per_day"].mean() if not sun_hours_daily.empty else None

    # Monthly total load
    df["month"] = df["datetime"].dt.to_period("M")
    monthly_total_load = df.groupby("month")["load"].sum().reset_index()
    monthly_total_load = monthly_total_load.rename(columns={"load": "total_load"})

    # Save outputs
    monthly_out = os.path.join(out_dir, f"{system_id}_monthly_total_load.csv")
    sun_out = os.path.join(out_dir, f"{system_id}_sun_hours_daily.csv")

    monthly_total_load.to_csv(monthly_out, index=False)
    sun_hours_daily.to_csv(sun_out, index=False)

    return {
        'monthly_out': monthly_out,
        'sun_hours_out': sun_out,
        'avg_sun_hours': avg_sun_hours
    }


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--raw', default='raw_energy_load.csv')
    parser.add_argument('--system-id', required=True)
    parser.add_argument('--start', default='2024-01-01')
    parser.add_argument('--end', default=datetime.utcnow().strftime('%Y-%m-%d'))
    parser.add_argument('--out-dir', default='.')
    args = parser.parse_args()
    res = run_compile_raw(args.raw, args.system_id, args.start, args.end, args.out_dir)
    if res:
        print(f"Wrote: {res}")