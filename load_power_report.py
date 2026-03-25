
import argparse
import csv
import json
import sys
from datetime import datetime
import math

try:
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.query import SimpleStatement
    from cassandra.cluster import NoHostAvailable
except Exception:
    print("Error: cassandra-driver is required. Install with: pip install cassandra-driver")
    sys.exit(1)

import yaml

def read_config():
    try:
        with open("conf.yaml", "r") as f:
            cfg = yaml.safe_load(f)
    except Exception:
        cfg = {}
    sc = cfg.get("scylla", {})
    host = sc.get("host", "127.0.0.1")
    if host == "localhost":
        host = "127.0.0.1"
    return {
        "hosts": [host] if isinstance(host, str) else host,
        "port": sc.get("port", 5533),
        "username": sc.get("username"),
        "password": sc.get("password"),
        "keyspace": sc.get("keyspace"),
        "try_for_times": int(sc.get("try_for_times", 5)),
    }

def connect_scylla(conf):
    auth = None
    if conf["username"] and conf["password"]:
        auth = PlainTextAuthProvider(username=conf["username"], password=conf["password"]) 

    cluster = None
    session = None
    last_exc = None
    for attempt in range(1, conf["try_for_times"] + 1):
        try:
            cluster = Cluster(contact_points=conf["hosts"], port=conf["port"], auth_provider=auth, connect_timeout=30)
            session = cluster.connect(keyspace=conf.get("keyspace"))
            return cluster, session
        except NoHostAvailable as e:
            last_exc = e
            print(f"NoHostAvailable (attempt {attempt}/{conf['try_for_times']}): {getattr(e,'errors', None)}")
        except Exception as e:
            last_exc = e
            print(f"Connection failed (attempt {attempt}/{conf['try_for_times']}): {e}")
    raise RuntimeError(f"Failed to connect to Scylla: {last_exc}")

def query_load_combined(session, system_id, start_date):
    cql = f"""
SELECT day, sum, count
FROM load_combined_1d
WHERE system_id = {system_id} AND day > '{start_date}';
"""
    stmt = SimpleStatement(cql)
    rows = session.execute(stmt)
    results = []
    for r in rows:
        try:
            row = r._asdict()
        except Exception:
            try:
                row = dict(r)
            except Exception:
                row = r
        s = row.get("sum")
        c = row.get("count")
        if s is None or c in (None, 0):
            continue
        val = float(s) / float(c)
        day = row.get("day")
        if isinstance(day, str):
            try:
                day = datetime.strptime(day, "%Y-%m-%d").date()
            except Exception:
                day = None
        results.append({"day": day, "value": val})
    return results

def analyze_and_write(system_id, rows, out_csv):
    if not rows:
        print("No data returned from Scylla for given system_id and date range.")
        return

    yearly = {}
    monthly = {}
    summer_months = {6,7,8}
    winter_months = {12,1,2}

    for r in rows:
        day = r["day"]
        if not day:
            continue
        val = r["value"]
        year = day.year
        ym = f"{day.year}-{day.month:02d}"
        yearly.setdefault(year, []).append(val)
        monthly.setdefault(ym, []).append(val)

    yearly_peaks = {y: max(vals) for y, vals in yearly.items() if vals}
    monthly_peaks = {m: max(vals) for m, vals in monthly.items() if vals}

    summer_vals = [v for r in rows if r["day"] and r["day"].month in summer_months for v in [r["value"]]]
    winter_vals = [v for r in rows if r["day"] and r["day"].month in winter_months for v in [r["value"]]]

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

    fieldnames = [
        "system_id",
        "start_date",
        "end_date",
        "overall_yearly_peak",
        "overall_monthly_peak",
        "summer_peak",
        "winter_peak",
        "growth_factor",
        "yearly_peaks",
        "monthly_peaks",
    ]

    start = min(r["day"] for r in rows if r["day"])
    end = max(r["day"] for r in rows if r["day"])

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

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(row)
    print(f"Wrote summary to {out_csv}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--system-id", required=True)
    parser.add_argument("--start", default="2024-01-01")
    parser.add_argument("--out", default="load-power.csv")
    args = parser.parse_args()

    conf = read_config()
    cluster, session = None, None
    try:
        cluster, session = connect_scylla(conf)
        rows = query_load_combined(session, args.system_id, args.start)
        analyze_and_write(args.system_id, rows, args.out)
    finally:
        if session:
            session.shutdown()
        if cluster:
            cluster.shutdown()

if __name__ == "__main__":
    main()