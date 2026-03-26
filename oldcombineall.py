#!/usr/bin/env python3
import argparse
import csv
import json
import os
from datetime import datetime
import yaml
import pandas as pd
import shutil

# Try to import local modules; fall back if not available
try:
    import apiwork
except Exception:
    apiwork = None

try:
    import query_system
except Exception:
    query_system = None

# try:
#     # import bms_soc
# except Exception:
#     bms_soc = None

try:
    import load_power_report
except Exception:
    load_power_report = None

# try:
#     import bill as bill_module
# except Exception:
#     bill_module = None

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

# try:
#     # grid_power_api integration disabled
#     grid_power_api = None
# except Exception:
#     grid_power_api = None


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
        res = query_system.query_system(system_id)
        return res or {}
    except Exception as e:
        print(f"Warning: query_system.query_system failed: {e}")
        return {}


# def safe_query_scylla_energy(system_id, limit=100):
#     if query_system is None:
#         print("Warning: query_system module not available. Skipping Scylla energy query.")
#         return []
#     try:
#         rows = query_system.query_scylla(system_id, limit=limit)
#         return rows or []
#     except Exception as e:
#         print(f"Warning: query_system.query_scylla failed: {e}")
#         return []


# def safe_bms_soc_summary(system_id, start_date, out_path):
#     # bms_soc integration disabled; return None as stub
#     print("Note: bms_soc integration is disabled in this build.")
#     return None


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


# async def try_capture_bill(city, ref_number, out_pdf, out_text):
#     # run bill capture if module present and async functions available
#     try:
#         import asyncio
#         if bill_module is None:
#             print("Warning: bill module not available. Skipping bill capture.")
#             return None
#         # Check functions exist
#         if not hasattr(bill_module, 'capture_bill_pdf'):
#             print("Warning: bill.capture_bill_pdf not found. Skipping bill capture.")
#             return None
#         ok = await bill_module.capture_bill_pdf(city, ref_number, out_pdf)
#         if not ok:
#             print("Warning: bill.capture_bill_pdf returned False")
#             return None
#         # try to extract text
#         if hasattr(bill_module, 'extract_text_from_pdf'):
#             text = await bill_module.extract_text_from_pdf(out_pdf)
#             with open(out_text, 'w', encoding='utf-8') as f:
#                 f.write(text)
#             return out_text
#         else:
#             return None
#     except Exception as e:
#         print(f"Warning: bill capture failed: {e}")
#         return None


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


def build_combined_row(system_id, postgres, api_data, bms_csv, load_csv, monthly_csv, start, end):
    row = {
        'system_id': system_id,
        'start': start,
        'end': end,
        'postgres': json.dumps(postgres, default=str),
        'api_data': json.dumps(api_data, default=str),
        # 'scylla_energy_count': len(scylla_energy_rows) if scylla_energy_rows is not None else 0,
        'bms_csv': bms_csv,
        'load_csv': load_csv,
        'monthly_csv': monthly_csv,
        'bill_text': None
    }
    # if bill_text_path and os.path.exists(bill_text_path):
    #     try:
    #         with open(bill_text_path, 'r', encoding='utf-8') as f:
    #             row['bill_text'] = f.read()
    #     except Exception:
    #         row['bill_text'] = None
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

    # # 3. Scylla energy
    # scylla_energy_rows = safe_query_scylla_energy(system_id, limit=500)

    # 4. BMS SOC
    # bms_soc call disabled; skip and set bms_csv to None
    bms_out = None
    bms_csv = None

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

    # # 8.5 grid_power_api (optional) - get per-system grid summary CSV or values
    # grid_summary_csv = None
    # peak_power_import_value = None
    # avg_365_days_power = None
    # if grid_power_api is not None:
    #     try:
    #         # prefer a conventional function if present
    #         if hasattr(grid_power_api, 'run_grid_power'):
    #             res = grid_power_api.run_grid_power(system_id, start, end, out_dir)
    #             if isinstance(res, dict):
    #                 grid_summary_csv = res.get('csv') or res.get('grid_summary_csv')
    #             elif isinstance(res, str):
    #                 grid_summary_csv = res
    #         elif hasattr(grid_power_api, 'get_grid_summary'):
    #             res = grid_power_api.get_grid_summary(system_id, out_dir)
    #             if isinstance(res, dict):
    #                 peak_power_import_value = res.get('peak_import_power')
    #                 avg_365_days_power = res.get('avg_365_days')
    #                 grid_summary_csv = res.get('csv') or res.get('grid_summary_csv')
    #         else:
    #             # no callable API; assume it wrote grid_summary.csv into out_dir
    #             possible = os.path.join(out_dir, 'grid_summary.csv')
    #             if os.path.exists(possible):
    #                 grid_summary_csv = possible

    #         # If we got a CSV path, try to read values from it
    #         if grid_summary_csv and os.path.exists(grid_summary_csv):
    #             try:
    #                 import pandas as _pd
    #                 gdf = _pd.read_csv(grid_summary_csv)
    #                 # try to find row by system_id
    #                 row = None
    #                 if 'system_id' in gdf.columns:
    #                     matched = gdf[gdf['system_id'] == system_id]
    #                     if not matched.empty:
    #                         row = matched.iloc[0]
    #                     else:
    #                         row = gdf.iloc[0]
    #                 else:
    #                     row = gdf.iloc[0]

    #                 if 'peak_import_power' in row.index and peak_power_import_value is None:
    #                     try:
    #                         peak_power_import_value = float(row['peak_import_power'])
    #                     except Exception:
    #                         pass
    #                 if 'avg_365_days' in row.index and avg_365_days_power is None:
    #                     try:
    #                         avg_365_days_power = float(row['avg_365_days'])
    #                     except Exception:
    #                         pass
    #             except Exception:
    #                 pass
    #     except Exception as e:
    #         print(f"Warning: grid_power_api processing failed: {e}")
    # # grid_power_api disabled for this run; no grid CSV to read
    # grid_summary_csv = None
    # peak_power_import_value = None
    # avg_365_days_power = None

    # # 9. Bill capture (optional)
    # bill_text_path = None
    # if not args.no_bill and bill_module is not None and ref_number and city:
    #     try:
    #         import asyncio
    #         pdf_path = os.path.join(out_dir, f"{system_id}_bill.pdf")
    #         txt_path = os.path.join(out_dir, f"{system_id}_bill.txt")
    #         asyncio.run(try_capture_bill(city, ref_number, pdf_path, txt_path))
    #         if os.path.exists(txt_path):
    #             bill_text_path = txt_path
    #     except Exception as e:
    #         print(f"Warning: running bill capture failed: {e}")

    # 10. Build combined row and write final CSV named as system id
    combined = build_combined_row(system_id, postgres, api_data, bms_csv, load_csv, monthly_from_raw, start, end)
    # add additional file paths to row
    combined['raw_energy_csv'] = raw_csv
    combined['daily_energy_csv'] = daily_energy_csv
    combined['monthly_from_raw'] = monthly_from_raw
    combined['sun_hours_csv'] = sun_hours_csv
    combined['tosee_daily'] = tosee_daily
    combined['tosee_monthly'] = tosee_monthly
    # combined['grid_summary_csv'] = grid_summary_csv

    final_csv = os.path.join(out_dir, f"{system_id}.csv")
    fieldnames = list(combined.keys())
    with open(final_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(combined)

    print(f"Combined CSV written: {final_csv}")

    # --- Build summary CSV with requested metrics ---
    summary = {
        'system_id': system_id,
        'daily_avg_import': None,
        'daily_peak_import': None,
        'day_avg_import': None,
        'day_peak_import': None,
        'night_avg_import': None,
        'night_peak_import': None,
        'current_pv': None,
        'current_battery': None,
        'current_battery_power': None,
        'current_battery_soc': None,
        'new_battery_design_year': None,
        'battery_dod': None,
        'battery_efficiency': None,
        'sun_hours_per_day': None,
        'location_yield': None,
        'kwh': None,
        'peak_power_import_value': None,
        'avg_365_days_power': None,
        'avg_total_load': None,
        'avg_night_fraction': None,
        # toseeunits import-export fields (day vs night)
        # 'day_avg_import' and 'day_peak_import' populated below
        'actual_system_age': None,
        'inverter_capacity': None
    }

    # 1) import-export summary
    try:
        ie_path = None
        # prefer explicit path from toseeunits
        if toseeunits is not None and isinstance(tosee_daily, str):
            # try to find import-export in same out_dir
            possible = os.path.join(out_dir, 'import-export.csv')
            if os.path.exists(possible):
                ie_path = possible
        if ie_path is None and os.path.exists(os.path.join(out_dir, 'import-export.csv')):
            ie_path = os.path.join(out_dir, 'import-export.csv')
        if ie_path and os.path.exists(ie_path):
            ie_df = pd.read_csv(ie_path)
            # single-row summary expected
            if not ie_df.empty:
                row = ie_df.iloc[0]
                summary['daily_avg_import'] = float(row.get('daily_avg_import', summary['daily_avg_import']) or summary['daily_avg_import'])
                summary['daily_peak_import'] = float(row.get('daily_peak_import', summary['daily_peak_import']) or summary['daily_peak_import'])
                # daytime fields added by toseeunits
                try:
                    summary['day_avg_import'] = float(row.get('day_avg_import', summary['day_avg_import']) or summary['day_avg_import'])
                except Exception:
                    pass
                try:
                    summary['day_peak_import'] = float(row.get('day_peak_import', summary['day_peak_import']) or summary['day_peak_import'])
                except Exception:
                    pass
                summary['night_avg_import'] = float(row.get('night_avg_import', summary['night_avg_import']) or summary['night_avg_import'])
                summary['night_peak_import'] = float(row.get('night_peak_import', summary['night_peak_import']) or summary['night_peak_import'])
    except Exception as e:
        print(f"Warning: reading import-export.csv failed: {e}")

    # # 1.5) grid summary values (if available)
    # try:
    #     # if grid_summary_csv not provided or missing, search common locations
    #     candidates = []
    #     if grid_summary_csv:
    #         candidates.append(grid_summary_csv)
    #     candidates.extend([
    #         os.path.join(out_dir, 'grid_summary.csv'),
    #         os.path.join(out_dir, f"{system_id}_grid_summary.csv"),
    #         'grid_summary.csv',
    #         f"{system_id}_grid_summary.csv",
    #         os.path.join('.', 'grid_summary.csv')
    #     ])
    #     found = None
    #     for c in candidates:
    #         try:
    #             if c and os.path.exists(c):
    #                 found = c
    #                 break
    #         except Exception:
    #             continue
    #     if found:
    #         grid_summary_csv = found
    #     if grid_summary_csv and os.path.exists(grid_summary_csv):
    #         import pandas as _pd
    #         gdf = _pd.read_csv(grid_summary_csv)
    #         row = None
    #         if 'system_id' in gdf.columns:
    #             matched = gdf[gdf['system_id'] == system_id]
    #             if not matched.empty:
    #                 row = matched.iloc[0]
    #             else:
    #                 row = gdf.iloc[0]
    #         else:
    #             row = gdf.iloc[0]

            # # map values (support both exact and legacy names)
            # def get_float(keys):
            #     for k in keys:
            #         try:
            #             if k in row.index and row.get(k) is not None and str(row.get(k)) != 'nan':
            #                 return float(row.get(k))
            #         except Exception:
            #             continue
            #     return None

            # # legacy peak/import
            # if summary.get('peak_power_import_value') is None:
            #     summary['peak_power_import_value'] = get_float(['peak_import_power','peak_import_power_kw','peak_import_power_kw_grid'])
            # if summary.get('avg_365_days_power') is None:
            #     summary['avg_365_days_power'] = get_float(['avg_365_days','avg_365_days_grid'])

            # # grid-derived fields
            # summary['avg_daily_import_kwh_grid'] = get_float(['avg_daily_import_kwh','avg_daily_import_kwh_grid','avg_daily_import'])
            # summary['avg_day_import_kwh_grid'] = get_float(['avg_day_import_kwh','avg_day_import_kwh_grid','avg_day_import'])
            # summary['avg_night_import_kwh_grid'] = get_float(['avg_night_import_kwh','avg_night_import_kwh_grid','avg_night_import'])
            # summary['peak_daily_import_kwh_grid'] = get_float(['peak_daily_import_kwh','peak_daily_import_kwh_grid','peak_daily_import'])
            # summary['peak_day_import_kwh_grid'] = get_float(['peak_day_import_kwh','peak_day_import_kwh_grid','peak_day_import'])
            # summary['peak_night_import_kwh_grid'] = get_float(['peak_night_import_kwh','peak_night_import_kwh_grid','peak_night_import'])
            # summary['peak_import_power_kw_grid'] = get_float(['peak_import_power_kw','peak_import_power_kw_grid','peak_import_power','peak_import_power_grid'])
            # summary['avg_365_days_grid'] = get_float(['avg_365_days','avg_365_days_grid'])
    # except Exception as e:
    #     print(f"Warning: reading grid_summary.csv failed: {e}")

    # 2) sun hours from compilerawdata result or CSV
    try:
        if compilerawdata is not None and 'res' in locals() and isinstance(res, dict):
            avg_sun = res.get('avg_sun_hours')
            if avg_sun is not None:
                summary['sun_hours_per_day'] = float(avg_sun)
        # fallback: try sun_hours CSV
        if summary['sun_hours_per_day'] is None and sun_hours_csv and os.path.exists(sun_hours_csv):
            sh = pd.read_csv(sun_hours_csv)
            if 'sun_hours_per_day' in sh.columns:
                summary['sun_hours_per_day'] = float(sh['sun_hours_per_day'].mean())
    except Exception as e:
        print(f"Warning: extracting sun hours failed: {e}")

    # 3) current pv / battery / soc from postgres.console_object or api_data
    def extract_console(obj):
        try:
            if isinstance(obj, str):
                return json.loads(obj)
            if isinstance(obj, dict):
                return obj
        except Exception:
            return None
        return None

    try:
        console = None
        if isinstance(postgres, dict):
            console_raw = postgres.get('console_object') or postgres.get('console')
            if console_raw:
                console = extract_console(console_raw)
        # also check api_data
        if console is None and isinstance(api_data, dict):
            # apiwork may wrap in data.system or data.systemV1
            try:
                sys_node = api_data.get('data', {}).get('system') or (apiwork.get('system') if apiwork else None) or api_data.get('data', {}).get('systemV1')
                if isinstance(sys_node, dict):
                    # some APIs include 'system' with 'siteDetails' and 'pv' etc
                    if 'pv' in sys_node:
                        console = sys_node
            except Exception:
                pass

        if console:
            # many fields live nested; attempt multiple keys
            pv = None
            if 'pv' in console and isinstance(console['pv'], dict):
                pv = console['pv'].get('pvProducedToday') or console['pv'].get('power') or console.get('pvProducedToday')
            if pv is None:
                pv = console.get('pvProducedToday') or console.get('pvProduced') or postgres.get('pv_produced_last_hour') if isinstance(postgres, dict) else None
            if pv is not None:
                try:
                    # summary['current_pv'] = float(pv)
                    summary['current_pv_kw'] = float(postgres.get('pv_produced_last_hour'))
                    summary['current_pv'] = summary['current_pv_kw']
                except Exception:
                    pass

            # battery capacity and storedPower
            # battery design capacity
            try:
                summary['current_battery'] = float(postgres.get('batteries_capacity')) if isinstance(postgres, dict) and postgres.get('batteries_capacity') is not None else None
            except Exception:
                pass
            # stored power
            sp = None
            if 'storedPower' in console:
                sp = console.get('storedPower')
            elif 'stored_power' in console:
                sp = console.get('stored_power')
            if sp is not None:
                try:
                    summary['current_battery_power'] = float(sp)
                except Exception:
                    pass
            # SOC
            soc = None
            if 'chargePercentage' in console:
                soc = console.get('chargePercentage')
            elif 'battery_soc' in postgres and isinstance(postgres, dict):
                soc = postgres.get('battery_soc')
            if soc is not None:
                try:
                    summary['current_battery_soc'] = float(soc)
                except Exception:
                    pass
    except Exception as e:
        print(f"Warning: extracting console values failed: {e}")
    current_date = datetime.now().date()
    # 4) other battery metadata from postgres
    try:
        if isinstance(postgres, dict):
            summary['new_battery_design_year'] = postgres.get('warranty_expiry_date') or postgres.get('deployed_at')
            
            # efficiency unknown - leave null unless available
            summary['battery_efficiency'] = None
            # location yield from postgres field average_pv_production_near_by
            summary['location_yield'] = postgres.get('average_pv_production_near_by')
            ################3
            summary['new_battery_design_year'] = postgres.get('warranty_expiry_date') or postgres.get('deployed_at')
            summary['battery_dod'] = 100 - postgres.get('battery_soc')
            summary['battery_efficiency'] = 0.6
            summary['location_yield'] = postgres.get('average_pv_production_near_by')
            summary['current_battery_soc'] = postgres.get('battery_soc')
            summary['current_battery_power'] = postgres.get('batteries_capacity')
            summary['current_pv_kw'] = float(postgres.get('pv_produced_last_hour'))
            summary['current_pv'] = summary['current_pv_kw']
            summary['actual_system_age'] = None
            summary['inverter_capacity'] = postgres.get('inverters_capacity')
            summary['current_battery'] = postgres.get('batteries_capacity')
            try:
                live_date_raw = postgres.get('live_date')
                if live_date_raw:
                    # try parsing various formats to a date
                    try:
                        if isinstance(live_date_raw, (int, float)):
                            ld_dt = datetime.fromtimestamp(float(live_date_raw))
                        else:
                            try:
                                ld_dt = datetime.fromisoformat(str(live_date_raw))
                            except Exception:
                                ld_dt = pd.to_datetime(live_date_raw, errors='coerce')
                        if pd.notna(ld_dt):
                            # ensure date
                            ld_date = ld_dt.date() if isinstance(ld_dt, datetime) else pd.to_datetime(ld_dt).date()
                            summary['actual_system_age'] = (current_date - ld_date).days
                    except Exception:
                        summary['actual_system_age'] = None
                # inverter capacity
                summary['inverter_capacity'] = postgres.get('inverters_capacity')
            except Exception:
                pass
    except Exception:
        pass

    # 5) kwh estimate - try monthly total load
    try:
        kwh = None
        if monthly_from_raw and os.path.exists(monthly_from_raw):
            mt = pd.read_csv(monthly_from_raw)
            if 'total_load' in mt.columns:
                kwh = float(mt['total_load'].sum())
        elif monthly_from_raw is None and os.path.exists(os.path.join(out_dir, f"{system_id}_monthly_total_load.csv")):
            mt = pd.read_csv(os.path.join(out_dir, f"{system_id}_monthly_total_load.csv"))
            if 'total_load' in mt.columns:
                kwh = float(mt['total_load'].sum())
        # fallback check API
        if kwh is None and isinstance(api_data, dict):
            try:
                # try systemV1 totals or grid.currentMonthBill.totalUnitsConsumed
                grid = None
                if api_data.get('data') and api_data['data'].get('system') and api_data['data']['system'].get('siteDetails'):
                    # some responses embed grid info under 'system' -> 'siteDetails' etc
                    pass
                # try to parse api_data stringified fields
                # no reliable fallback
            except Exception:
                pass
        summary['kwh'] = kwh
    except Exception as e:
        print(f"Warning: computing kwh failed: {e}")

    # --- Compute avg daily total load and avg night fraction from energy_load outputs ---
    try:
        avg_total_load = None
        avg_night_fraction = None
        # prefer daily CSV produced by energy_load.run_energy_load
        if daily_energy_csv and os.path.exists(daily_energy_csv):
            try:
                ddf = pd.read_csv(daily_energy_csv)
                if 'total_load' in ddf.columns:
                    avg_total_load = float(ddf['total_load'].mean())
                if 'night_fraction' in ddf.columns:
                    avg_night_fraction = float(ddf['night_fraction'].mean())
            except Exception:
                pass
        else:
            # fallback: aggregate from raw hourly CSV
            if raw_csv and os.path.exists(raw_csv):
                try:
                    rdf = pd.read_csv(raw_csv, parse_dates=['datetime'])
                    rdf['date'] = pd.to_datetime(rdf['datetime']).dt.date
                    daily = rdf.groupby('date')['load'].sum().reset_index()
                    if 'load' in daily.columns:
                        avg_total_load = float(daily['load'].mean())
                    # compute night fraction if sunrise/sunset present
                    if 'sunrise' in rdf.columns and 'sunset' in rdf.columns:
                        try:
                            rdf['sunrise'] = pd.to_datetime(rdf['sunrise']).dt.time
                            rdf['sunset'] = pd.to_datetime(rdf['sunset']).dt.time
                        except Exception:
                            pass

                        def night_frac_group(g):
                            total = g['load'].sum()
                            if total <= 0:
                                return 0.0
                            first_sr = g['sunrise'].iloc[0]
                            first_ss = g['sunset'].iloc[0]
                            night = g[(pd.to_datetime(g['datetime']).dt.time < first_sr) | (pd.to_datetime(g['datetime']).dt.time > first_ss)]['load'].sum()
                            return round(night / total * 100, 2)

                        nf = rdf.groupby('date').apply(night_frac_group)
                        if not nf.empty:
                            avg_night_fraction = float(nf.mean())
                except Exception:
                    pass

        summary['avg_total_load'] = avg_total_load
        summary['avg_night_fraction'] = avg_night_fraction
    except Exception as e:
        print(f"Warning: computing avg load/night fraction failed: {e}")

    # write summary CSV
    try:
        summary_csv = os.path.join(out_dir, f"{system_id}_summary.csv")
        # ensure deterministic column order
        cols = ['system_id','daily_avg_import','daily_peak_import','day_avg_import','day_peak_import','night_avg_import','night_peak_import','current_pv','current_battery','current_battery_power','current_battery_soc','new_battery_design_year','battery_dod','battery_efficiency','sun_hours_per_day','location_yield','avg_total_load','avg_night_fraction','actual_system_age','inverter_capacity']
        with open(summary_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=cols)
            writer.writeheader()
            writer.writerow({k: summary.get(k) for k in cols})
        print(f"Summary CSV written: {summary_csv}")
    except Exception as e:
        print(f"Warning: writing summary CSV failed: {e}")

    # move CSVs except summary into saved folder
    try:
        import glob
        saved_folder = os.path.join(out_dir, f"saved_csvs")
        os.makedirs(saved_folder, exist_ok=True)

        # move all CSVs in out_dir except the summary_csv
        for p in glob.glob(os.path.join(out_dir, "*.csv")):
            try:
                if os.path.abspath(p) == os.path.abspath(summary_csv):
                    continue
                shutil.move(p, saved_folder)
            except Exception:
                # ignore individual move failures
                pass

        # also move tracked csv paths that may be outside out_dir
        tracked = [
            final_csv,
            combined.get('raw_energy_csv'),
            combined.get('daily_energy_csv'),
            combined.get('monthly_from_raw'),
            combined.get('sun_hours_csv'),
            combined.get('tosee_daily'),
            combined.get('tosee_monthly'),
            bms_csv,
            load_csv
        ]
        for tp in tracked:
            try:
                if tp and isinstance(tp, str) and os.path.exists(tp):
                    # skip summary CSV explicitly
                    if os.path.abspath(tp) == os.path.abspath(summary_csv):
                        continue
                    dest = os.path.join(saved_folder, os.path.basename(tp))
                    # if already in saved_folder skip
                    if os.path.abspath(os.path.dirname(tp)) == os.path.abspath(saved_folder):
                        continue
                    shutil.move(tp, dest)
            except Exception:
                pass

        print(f"Moved CSV files (except summary) into: {saved_folder}")
    except Exception as e:
        print(f"Warning: cleaning up CSVs failed: {e}")

    try:
        saved_ones = os.path.join(out_dir, 'saved_csvs')

        if os.path.exists(saved_ones):
            shutil.rmtree(saved_ones, ignore_errors=True)
            print(f"Deleted folder (if it existed): {saved_ones}")
        else:
            print(f"Folder does not exist: {saved_ones}")
        # # delete legacy folder named '_saved_ones' if present
        # saved_ones = os.path.join(out_dir, '_saved_ones')
        # if os.path.exists(saved_ones) and os.path.isdir(saved_ones):
        #     try:
        #         shutil.rmtree(saved_ones)
        #         print(f"Deleted legacy folder: {saved_ones}")
        #     except Exception as e:
        #         print(f"Warning: failed to delete {saved_ones}: {e}")
    except Exception:
        pass    


if __name__ == '__main__':
    main()
