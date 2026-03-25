#!/usr/bin/env python3
import argparse
import csv
import json
import os
from datetime import datetime
import yaml
import pandas as pd

# Try to import local modules; fall back if not available
try:
    import apiwork
except Exception:
    apiwork = None

try:
    import query_system
except Exception:
    query_system = None

try:
    import bms_soc
except Exception:
    bms_soc = None

try:
    import load_power_report
except Exception:
    load_power_report = None

try:
    import bill as bill_module
except Exception:
    bill_module = None

try:
    import energy_load
except Exception:
    energy_load = None

try:
    import compilerawdata
except Exception:
    compilerawdata = None

try:
    import toseeunits
except Exception:
    toseeunits = None


def safe_get_system_details(system_id):
    if apiwork is None:
        print("Warning: apiwork module not available. Skipping API system details.")
        return {}
    try:
        res = apiwork.get_system_details(system_id)
        return res or {}
    except Exception as e:
        print(f"Warning: get_system_details failed: {e}")
        return {}


def safe_query_postgres(system_id):
    if query_system is None:
        print("Warning: query_system module not available. Skipping Postgres query.")
        return {}
    try:
        # ignore deployed_before filter by passing a far future date
        res = query_system.query_system(system_id, deployed_before='2100-01-01')
        return res or {}
    except Exception as e:
        print(f"Warning: query_system.query_system failed: {e}")
        return {}


def safe_query_scylla_energy(system_id, limit=100):
    if query_system is None:
        print("Warning: query_system module not available. Skipping Scylla energy query.")
        return []
    try:
        rows = query_system.query_scylla(system_id, limit=limit)
        return rows or []
    except Exception as e:
        print(f"Warning: query_system.query_scylla failed: {e}")
        return []


def safe_bms_soc_summary(system_id, start_date, out_path):
    if bms_soc is None:
        print("Warning: bms_soc module not available. Skipping BMS SOC summary.")
        return None
    try:
        conf = bms_soc.read_config()
        cluster, session = None, None
        try:
            cluster, session = bms_soc.connect_scylla(conf)
            rows = bms_soc.query_bms_soc(session, system_id, start_date)
            # write to a temporary CSV with system id name
            bms_out = out_path
            bms_soc.analyze_and_write(system_id, rows, bms_out)
            return bms_out
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
    except Exception as e:
        print(f"Warning: BMS SOC processing failed: {e}")
        return None


def safe_load_power_summary(system_id, start_date, out_path):
    if load_power_report is None:
        print("Warning: load_power_report module not available. Skipping load power summary.")
        return None
    try:
        conf = load_power_report.read_config()
        cluster, session = None, None
        try:
            cluster, session = load_power_report.connect_scylla(conf)
            rows = load_power_report.query_load_combined(session, system_id, start_date)
            load_power_report.analyze_and_write(system_id, rows, out_path)
            return out_path
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
    except Exception as e:
        print(f"Warning: load power processing failed: {e}")
        return None


async def try_capture_bill(city, ref_number, out_pdf, out_text):
    # run bill capture if module present and async functions available
    try:
        import asyncio
        if bill_module is None:
            print("Warning: bill module not available. Skipping bill capture.")
            return None
        # Check functions exist
        if not hasattr(bill_module, 'capture_bill_pdf'):
            print("Warning: bill.capture_bill_pdf not found. Skipping bill capture.")
            return None
        ok = await bill_module.capture_bill_pdf(city, ref_number, out_pdf)
        if not ok:
            print("Warning: bill.capture_bill_pdf returned False")
            return None
        # try to extract text
        if hasattr(bill_module, 'extract_text_from_pdf'):
            text = await bill_module.extract_text_from_pdf(out_pdf)
            with open(out_text, 'w', encoding='utf-8') as f:
                f.write(text)
            return out_text
        else:
            return None
    except Exception as e:
        print(f"Warning: bill capture failed: {e}")
        return None


def compile_monthly_from_raw(raw_path, monthly_out):
    # replicate logic from compilerawdata.py
    if not os.path.exists(raw_path):
        print(f"Warning: raw hourly file {raw_path} not found. Skipping monthly compile.")
        return None
    try:
        df = pd.read_csv(raw_path, parse_dates=["datetime"])
        # handle sunrise/sunset parsing robustly
        try:
            df['sunrise'] = pd.to_datetime(df['sunrise']).dt.time
            df['sunset'] = pd.to_datetime(df['sunset']).dt.time
        except Exception:
            # ignore and continue
            pass
        df['month'] = df['datetime'].dt.to_period('M')
        monthly_total_load = df.groupby('month')['load'].sum().reset_index()
        monthly_total_load = monthly_total_load.rename(columns={'load': 'total_load'})
        monthly_total_load.to_csv(monthly_out, index=False)
        return monthly_out
    except Exception as e:
        print(f"Warning: compiling monthly from raw failed: {e}")
        return None


def read_conf_defaults(conf_path='conf.yaml'):
    try:
        with open(conf_path, 'r') as f:
            cfg = yaml.safe_load(f)
            defaults = cfg.get('defaults', {}) if cfg else {}
            return defaults.get('system_id'), defaults.get('start_date'), defaults.get('end_date')
    except Exception:
        return None, None, None


def build_combined_row(system_id, postgres, api_data, scylla_energy_rows, bms_csv, load_csv, monthly_csv, bill_text_path, start, end):
    row = {
        'system_id': system_id,
        'start': start,
        'end': end,
        'postgres': json.dumps(postgres, default=str),
        'api_data': json.dumps(api_data, default=str),
        'scylla_energy_count': len(scylla_energy_rows) if scylla_energy_rows is not None else 0,
        'bms_csv': bms_csv,
        'load_csv': load_csv,
        'monthly_csv': monthly_csv,
        'bill_text': None
    }
    if bill_text_path and os.path.exists(bill_text_path):
        try:
            with open(bill_text_path, 'r', encoding='utf-8') as f:
                row['bill_text'] = f.read()
        except Exception:
            row['bill_text'] = None
    return row


def main():
    parser = argparse.ArgumentParser(description='Combine data from various modules into a single CSV named by system id')
    parser.add_argument('--system-id', required=False)
    parser.add_argument('--start', required=False, help='Start date YYYY-MM-DD')
    parser.add_argument('--end', required=False, help='End date YYYY-MM-DD')
    parser.add_argument('--out-dir', default='.', help='Output directory')
    parser.add_argument('--no-bill', action='store_true', help='Skip attempting to fetch bill via browser')
    args = parser.parse_args()

    # read defaults from conf.yaml if values not provided
    def_sid, def_start, def_end = read_conf_defaults()
    system_id = args.system_id or def_sid
    start = args.start or def_start
    end = args.end or def_end

    if not system_id or not start or not end:
        raise SystemExit('system-id, start and end must be provided either via CLI or conf.yaml defaults')

    out_dir = args.out_dir
    os.makedirs(out_dir, exist_ok=True)

    # 1. API system details
    api_data = safe_get_system_details(system_id)

    # Try to extract city and reference number
    city = None
    ref_number = None
    try:
        sd = None
        if isinstance(api_data, dict):
            sd = api_data.get('data', {}).get('system', {}).get('siteDetails') if api_data.get('data') else api_data.get('system', {}).get('siteDetails')
            if not sd and 'system' in api_data:
                sd = api_data['system'].get('siteDetails')
            if sd:
                if isinstance(sd, dict):
                    city = sd.get('city', {}).get('name') if sd.get('city') else sd.get('city')
                    ref_number = sd.get('referenceNumber') or sd.get('referenceNumber')
                elif isinstance(sd, list) and sd:
                    city = sd[0].get('city', {}).get('name') if sd[0].get('city') else None
                    ref_number = sd[0].get('referenceNumber')
    except Exception:
        pass

    # 2. Postgres details
    postgres = safe_query_postgres(system_id)

    # if city not found, try postgres
    if not city:
        try:
            city = postgres.get('location') or (postgres.get('address') if isinstance(postgres, dict) else None)
        except Exception:
            city = None

    # 3. Scylla energy
    scylla_energy_rows = safe_query_scylla_energy(system_id, limit=500)

    # 4. BMS SOC
    bms_out = os.path.join(out_dir, f"{system_id}_bms_soc_summary.csv")
    bms_csv = safe_bms_soc_summary(system_id, start, bms_out)

    # 5. Load power
    load_out = os.path.join(out_dir, f"{system_id}_load_power_summary.csv")
    load_csv = safe_load_power_summary(system_id, start, load_out)

    # 6. Energy load (now writes raw CSV prefixed by system id)
    raw_csv = None
    daily_energy_csv = None
    if energy_load is not None:
        try:
            res = energy_load.run_energy_load(system_id, start, end, out_dir)
            raw_csv = res.get('raw_csv') if res else None
            daily_energy_csv = res.get('daily_csv') if res else None
        except Exception as e:
            print(f"Warning: energy_load.run_energy_load failed: {e}")

    # 7. Compile raw using compilerawdata module
    monthly_from_raw = None
    sun_hours_csv = None
    if compilerawdata is not None and raw_csv:
        try:
            res = compilerawdata.run_compile_raw(raw_csv, system_id, start, end, out_dir)
            if res:
                monthly_from_raw = res.get('monthly_out')
                sun_hours_csv = res.get('sun_hours_out')
        except Exception as e:
            print(f"Warning: compilerawdata.run_compile_raw failed: {e}")
    else:
        # fallback: try previous compile_monthly_from_raw behavior on default raw filename
        monthly_fallback = os.path.join(out_dir, f"{system_id}_monthly_total_load.csv")
        monthly_from_raw = compile_monthly_from_raw('raw_energy_load.csv', monthly_fallback)

    # 8. toseeunits (daily/monthly solar CSVs)
    tosee_daily = None
    tosee_monthly = None
    if toseeunits is not None:
        try:
            res = toseeunits.run_tosee_units(system_id, start, end, out_dir)
            if res:
                tosee_daily = res.get('daily_csv')
                tosee_monthly = res.get('monthly_csv')
        except Exception as e:
            print(f"Warning: toseeunits.run_tosee_units failed: {e}")

    # 9. Bill capture (optional)
    bill_text_path = None
    if not args.no_bill and bill_module is not None and ref_number and city:
        try:
            import asyncio
            pdf_path = os.path.join(out_dir, f"{system_id}_bill.pdf")
            txt_path = os.path.join(out_dir, f"{system_id}_bill.txt")
            asyncio.run(try_capture_bill(city, ref_number, pdf_path, txt_path))
            if os.path.exists(txt_path):
                bill_text_path = txt_path
        except Exception as e:
            print(f"Warning: running bill capture failed: {e}")

    # 10. Build combined row and write final CSV named as system id
    combined = build_combined_row(system_id, postgres, api_data, scylla_energy_rows, bms_csv, load_csv, monthly_from_raw, bill_text_path, start, end)
    # add additional file paths to row
    combined['raw_energy_csv'] = raw_csv
    combined['daily_energy_csv'] = daily_energy_csv
    combined['monthly_from_raw'] = monthly_from_raw
    combined['sun_hours_csv'] = sun_hours_csv
    combined['tosee_daily'] = tosee_daily
    combined['tosee_monthly'] = tosee_monthly

    final_csv = os.path.join(out_dir, f"{system_id}.csv")
    fieldnames = list(combined.keys())
    with open(final_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(combined)

    print(f"Combined CSV written: {final_csv}")


if __name__ == '__main__':
    main()
