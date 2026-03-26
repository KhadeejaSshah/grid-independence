#!/usr/bin/env python3
import argparse
import json
import psycopg2.extras
import database
import yaml
import time
import uuid
import psycopg2.extras

def query_system(system_id):
    conn = None
    try:
        conn = database.get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        query = """
        SELECT 
            s.system_no,
            s.id,
            s.name,
            s.customer_name,
            s.location,
            sd.address,

            s.state,
            s.smart_flow_enabled,
            s.is_export_enabled,
            s.export_type,
            s.type,
            s.system_type,

            s.battery_discharge_limit,
            s.backup_in_hours,
            s.ssg_version,
            si.is_vip,

            s.region,
            s.down_date AT TIME ZONE 'GMT+5' AS down_date,
            s.under_maintenance_date AT TIME ZONE 'GMT+5' AS under_maintenance_date,

            sd.is_net_metering_activated,

            s.inverter_model,
            (s.panels_capacity / 1000) AS panels_capacity,
            (s.inverters_capacity * s.inverters_count) AS inverters_capacity,
            s.batteries_capacity,

            s.pv_produced_last_hour,
            s.pv_produced_last_24hours,

            s.soc AS battery_soc,
            s.battery_model,
            s.batteries_count,

            s.order_id,
            s.disconnected,
            s.disconnected_at AT TIME ZONE 'GMT+5' AS disconnected_at,

            s.deployed_at AT TIME ZONE 'GMT+5' AS deployed_at,
            s.warranty_expiry_date,
            s.pm_date,
            s.live_date,

            sd.noc_services_expiry_date,

            -- Power + Tariff + Feeder
            pc.short_name AS power_company,
            t.id AS tariff_id,
            t.name AS tariff_name,
            t.type AS tariff_type,
            f.id AS feeder_id,
            f.name AS feeder_name,

            -- PV calculation
            ((pv.average_pv_production * 365) * 0.90) AS average_pv_production_near_by

        FROM systems s

        INNER JOIN system_site_details sd
            ON s.id = sd.system_id

        INNER JOIN system_console_objects sc
            ON s.id = sc.system_id

        INNER JOIN system_info si
            ON s.id = si.system_id

        LEFT JOIN tariffs t
            ON t.id = sd.tariff_id

        LEFT JOIN feeders f
            ON f.id = sd.feeder_id

        LEFT JOIN power_companies pc
            ON pc.id = f.power_company_id

        LEFT JOIN (
            SELECT DISTINCT city, average_pv_production
            FROM dashboad_pv_analysis
            WHERE average_pv_production IS NOT NULL
              AND average_pv_production <> 0
        ) pv
            ON pv.city = s.location

        WHERE s.id = %s;
        """

        cur.execute(query, (system_id,))
        result = cur.fetchone()

        cur.close()
        return result

    finally:
        if conn:
            database.release_db_connection(conn)



def query_scylla(system_id, limit=50):
    # Try to import cassandra driver; if missing, skip Scylla query but keep script runnable
    try:
        from cassandra.cluster import Cluster, NoHostAvailable
        from cassandra.auth import PlainTextAuthProvider
        from cassandra.query import SimpleStatement
    except Exception:
        print("Warning: cassandra-driver not installed. Skipping Scylla query. Install with: pip install cassandra-driver")
        return []

    # read scylla config
    try:
        with open("conf.yaml", "r") as f:
            cfg = yaml.safe_load(f)
    except Exception as e:
        print(f"Warning: failed to read conf.yaml: {e}. Using defaults for Scylla connection.")
        cfg = {}

    sc = cfg.get("scylla", {})
    hosts = sc.get("host", "127.0.0.1")
    if isinstance(hosts, str):
        hosts = [hosts]
    # prefer 127.0.0.1 for tunnel
    hosts = [h if h != 'localhost' else '127.0.0.1' for h in hosts]
    port = sc.get("port", 5533)
    username = sc.get("username")
    password = sc.get("password")
    try_for_times = int(sc.get("try_for_times", 5)) if sc.get("try_for_times") is not None else 5
    keyspace = sc.get("keyspace")

    auth_provider = None
    if username and password:
        auth_provider = PlainTextAuthProvider(username=username, password=password)

    last_exc = None
    cluster = None
    session = None
    for attempt in range(1, try_for_times + 1):
        try:
            cluster = Cluster(contact_points=hosts, port=port, auth_provider=auth_provider, connect_timeout=30)
            if keyspace:
                session = cluster.connect(keyspace=keyspace)
            else:
                session = cluster.connect()
            break
        except NoHostAvailable as e:
            last_exc = e
            # print diagnostics
            print(f"NoHostAvailable on attempt {attempt}/{try_for_times}. Errors: {getattr(e, 'errors', None)}")
            time.sleep(1)
        except Exception as e:
            last_exc = e
            print(f"Scylla connection attempt {attempt}/{try_for_times} failed: {e}")
            time.sleep(1)
    else:
        print(f"Failed to connect to Scylla after {try_for_times} attempts: {last_exc}")
        # ensure cleanup
        try:
            if cluster:
                cluster.shutdown()
        except Exception:
            pass
        return []

    try:
        cql = """
SELECT system_id, day, grid_consumed, load
FROM energy_stats_1d
WHERE system_id = %s
ORDER BY day DESC
LIMIT %s
"""
        stmt = SimpleStatement(cql)
        # convert system_id string to uuid.UUID so the driver binds it as UUID type
        try:
            uuid_obj = uuid.UUID(system_id)
        except Exception as e:
            print(f"Invalid system_id UUID: {e}")
            return []
        rows = session.execute(stmt, (uuid_obj, limit))
        # convert rows to list of dicts safely
        results = []
        for row in rows:
            try:
                results.append(dict(row._asdict()))
            except Exception:
                # row may already be a dict due to driver settings
                try:
                    results.append(dict(row))
                except Exception:
                    results.append(row)
        return results
    except Exception as e:
        print(f"Error executing Scylla query: {e}")
        return []
    finally:
        try:
            if session:
                session.shutdown()
        except Exception:
            pass
        try:
            if cluster:
                cluster.shutdown()
        except Exception:
            pass


if __name__ == "__main__":
    # Read system_id from conf.yaml defaults
    try:
        with open("conf.yaml", "r") as f:
            cfg = yaml.safe_load(f) or {}
        defaults = cfg.get("defaults", {})
        system_id = defaults.get("system_id")
    except Exception as e:
        print(f"Error: could not read conf.yaml: {e}")
        raise SystemExit("conf.yaml is required with defaults.system_id")

    if not system_id:
        raise SystemExit("defaults.system_id is required in conf.yaml")

    try:
        res = query_system(system_id)
        if res:
            print("Postgres:")
            print(json.dumps(res, default=str, indent=2))
        else:
            print("No matching system found in Postgres")

        print("\nScylla last 5 energy_stats_1d rows:")
        # scylla_rows = query_scylla(system_id, limit=5)
        # if scylla_rows:
        #     print(json.dumps(scylla_rows, default=str, indent=2))
        # else:
        #     print("No rows in Scylla for given system_id")
    except Exception as e:
        print(f"Error: {e}")
        raise
