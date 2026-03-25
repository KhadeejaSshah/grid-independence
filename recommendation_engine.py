import os
import json
import pandas as pd
import yaml
import google.generativeai as genai
from typing import Dict, Any, Optional
import query_system as qs

def load_config():
    with open("conf.yaml", "r") as f:
        return yaml.safe_load(f)

def gather_system_data(system_id: str, out_dir: str = ".") -> Dict[str, Any]:
    """
    Gathers all available system data directly from Postgres (via query_system)
    and any pre-existing CSV files. No subprocess calls.
    """
    print(f"\n[DEBUG] {'='*20} GATHERING DATA: {system_id} {'='*20}")
    data = {"system_id": system_id}

    # 1. Postgres
    print(f"[DEBUG] Fetching Postgres data for {system_id}...")
    try:
        pg = qs.query_system(system_id)
        if pg:
            print(f"[DEBUG] Postgres Success: Found system '{pg.get('name')}'")
            data["specs"] = {
                "name": pg.get("name"),
                "customer": pg.get("customer_name"),
                "location": pg.get("location"),
                "address": pg.get("address"),
                "system_no": pg.get("system_no"),
                "panels_capacity_kw": pg.get("panels_capacity"),
                "inverters_capacity_kw": pg.get("inverters_capacity"),
                "current_battery_kwh": pg.get("current_battery_kwh"),
                "current_pv_kw": pg.get("current_pv_kw"),
                "pv_produced_last_24h": pg.get("pv_produced_last_24hours"),
                "battery_soc": pg.get("battery_soc"),
                "battery_model": pg.get("battery_model"),
                "batteries_count": pg.get("batteries_count"),
                "inverter_model": pg.get("inverter_model"),
                "battery_discharge_limit": pg.get("battery_discharge_limit"),
                "backup_hours": pg.get("backup_in_hours"),
                "deployed_at": str(pg.get("deployed_at") or ""),
                "warranty_expiry_date": str(pg.get("warranty_expiry_date") or ""),
                "is_net_metering": pg.get("is_net_metering_activated"),
                "power_company": pg.get("power_company"),
                "tariff_name": pg.get("tariff_name"),
                "location_yield": pg.get("average_pv_production_near_by"),
                "state": pg.get("state"),
                "region": pg.get("region"),
            }
            # print(f"[DEBUG] Specs: {json.dumps(data['specs'], indent=2, default=str)}")
        else:
            print(f"[DEBUG] Postgres: No data found for {system_id}")
    except Exception as e:
        print(f"[DEBUG] Postgres ERROR: {e}")

    # 2. Import-export summary CSV
    ie_path = os.path.join(out_dir, "import-export.csv")
    print(f"[DEBUG] Checking CSV: {ie_path}")
    if os.path.exists(ie_path):
        try:
            df_ie = pd.read_csv(ie_path)
            if not df_ie.empty:
                row = df_ie.iloc[0]
                data["import_export"] = {
                    "daily_avg_import": _safe_float(row.get("daily_avg_import")),
                    "daily_peak_import": _safe_float(row.get("daily_peak_import")),
                    "night_avg_import": _safe_float(row.get("night_avg_import")),
                    "night_peak_import": _safe_float(row.get("night_peak_import")),
                }
                print(f"[DEBUG] CSV Found (import-export): {data['import_export']}")
            else:
                print(f"[DEBUG] CSV Empty: {ie_path}")
        except Exception as e:
            print(f"[DEBUG] CSV ERROR (import-export): {e}")
    else:
        print(f"[DEBUG] CSV Missing: {ie_path}")

    # 3. Monthly dashboard data CSV
    monthly_path = os.path.join(out_dir, f"{system_id}_dashboard-data-monthly.csv")
    print(f"[DEBUG] Checking CSV: {monthly_path}")
    if os.path.exists(monthly_path):
        try:
            df_m = pd.read_csv(monthly_path)
            data["monthly_history"] = df_m.to_dict(orient="records")
            print(f"[DEBUG] CSV Found (monthly): {len(data['monthly_history'])} records")
        except Exception as e:
            print(f"[DEBUG] CSV ERROR (monthly): {e}")
    else:
        print(f"[DEBUG] CSV Missing: {monthly_path}")

    # 4. BMS SOC summary CSV
    bms_path = os.path.join(out_dir, f"{system_id}_bms_soc_summary.csv")
    print(f"[DEBUG] Checking CSV: {bms_path}")
    if os.path.exists(bms_path):
        try:
            df_bms = pd.read_csv(bms_path)
            if not df_bms.empty:
                data["bms_health"] = df_bms.iloc[0].to_dict()
                print(f"[DEBUG] CSV Found (BMS): {data['bms_health']}")
            else:
                print(f"[DEBUG] CSV Empty: {bms_path}")
        except Exception as e:
            print(f"[DEBUG] CSV ERROR (BMS): {e}")
    else:
        print(f"[DEBUG] CSV Missing: {bms_path}")

    # 5. Summary CSV
    summary_path = os.path.join(out_dir, f"{system_id}_summary.csv")
    print(f"[DEBUG] Checking CSV: {summary_path}")
    if os.path.exists(summary_path):
        try:
            df_s = pd.read_csv(summary_path)
            if not df_s.empty:
                row = df_s.iloc[0]
                data["summary_metrics"] = {
                    "daily_avg_import": _safe_float(row.get("daily_avg_import")),
                    "night_avg_import": _safe_float(row.get("night_avg_import")),
                    "sun_hours_per_day": _safe_float(row.get("sun_hours_per_day")),
                    "total_kwh": _safe_float(row.get("kwh")),
                    "location_yield": _safe_float(row.get("location_yield")),
                    "battery_dod": _safe_float(row.get("battery_dod")),
                }
                print(f"[DEBUG] CSV Found (summary): {data['summary_metrics']}")
            else:
                print(f"[DEBUG] CSV Empty: {summary_path}")
        except Exception as e:
            print(f"[DEBUG] CSV ERROR (summary): {e}")
    else:
        print(f"[DEBUG] CSV Missing: {summary_path}")

    print(f"[DEBUG] {'-'*10} Data Gathering Complete {'-'*10}\n")
    return data


def _safe_float(val):
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        return float(val)
    except (ValueError, TypeError):
        return None


def get_ai_recommendation(system_data: Dict[str, Any], target_independence: int) -> Dict[str, Any]:
    """
    Calls Gemini API to get structured recommendations.
    """
    print(f"\n[AI] {'='*20} STARTING RECOMMENDATION {'='*20}")
    print(f"[AI] Target Independence: {target_independence}%")
    
    cfg = load_config()
    api_key = cfg.get("secrets", {}).get("GEMINI_API_KEY")
    if not api_key:
        print("[AI] ERROR: Gemini API Key missing")
        return {"error": "Gemini API Key missing in conf.yaml"}

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(
        'gemini-2.0-flash',
        generation_config=genai.types.GenerationConfig(temperature=0)
    )

    # Pre-calculate current grid independence from real data
    print("[AI] Running pre-calculations...")
    ie = system_data.get("import_export") or system_data.get("summary_metrics") or {}
    specs = system_data.get("specs") or {}
    
    daily_import = _safe_float(ie.get("daily_avg_import")) or 0
    pv_kw = _safe_float(specs.get("panels_capacity_kw")) or 0
    sun_hours = _safe_float(ie.get("sun_hours_per_day") or (system_data.get("summary_metrics") or {}).get("sun_hours_per_day")) or 5
    
    daily_production = pv_kw * sun_hours * 0.8  # estimated daily solar output
    daily_load = daily_production + daily_import  # total load ≈ what solar covers + what grid covers
    current_gi = round(((daily_load - daily_import) / daily_load * 100), 1) if daily_load > 0 else 0
    target_met = current_gi >= target_independence

    print(f"[AI] Pre-calc values: import={daily_import}, pv_kw={pv_kw}, sun_hours={sun_hours}")
    print(f"[AI] Pre-calc results: production={daily_production}, load={daily_load}, current_gi={current_gi}%, target_met={target_met}")

    prompt = f"""
As a Solar Energy Expert, analyze the following system data and provide recommendations to achieve {target_independence}% Grid Independence.
Grid independence = percentage of total load covered by Solar + Battery, minimizing grid import.

SYSTEM DATA:
{json.dumps(system_data, indent=2, default=str)}

PRE-CALCULATED FIGURES (USE THESE):
- Current Grid Independence: {current_gi}%
- Target: {target_independence}%
- Target already met: {"YES" if target_met else "NO"}
- Estimated Daily Solar Production: {round(daily_production, 2)} kWh
- Estimated Daily Load: {round(daily_load, 2)} kWh
- Daily Grid Import: {round(daily_import, 2)} kWh


Return ONLY a JSON object. Use numeric values.

CRITICAL LOGIC:
1. If "Target already met" is YES, you MUST set status to "OK" and recommended equals current, UNLESS hardware is physically failing (BMS < 20% or system > 10 yrs).
2. Do NOT suggest upgrades to reach 100% if the target is 25%. Respect the user's target.
3. Avoid confusing "Panels Capacity" ({pv_kw} kW) with "Current PV Production".

JSON structure:
{{
  "current_grid_independence": {current_gi},
  "projected_grid_independence": <number>,
  "solar": {{
    "status": "<OK|Increase|Repair|Replace>",
    "action": "<short 3-5 word action>",
    "current_kw": {pv_kw},
    "recommended_kw": <recommended number>,
    "production_gain_kwh": <kWh gain>
  }},
  "battery": {{
    "status": "<OK|Increase|Repair|Replace>",
    "action": "<short 3-5 word action>",
    "current_kwh": {_safe_float(specs.get("current_battery_kwh")) or 0},
    "recommended_kwh": <recommended number>,
    "backup_hours_gain": <hours gain>
  }},
  "inverter": {{
    "status": "<OK|Upgrade|Repair|Replace>",
    "action": "<short 3-5 word action>",
    "current_kw": {_safe_float(specs.get("inverters_capacity_kw")) or 0},
    "recommended_kw": <recommended number>
  }},
  "grid_impact": {{
    "current_daily_import_kwh": {daily_import},
    "projected_daily_import_kwh": <projected>,
    "current_night_import_kwh": {_safe_float(ie.get("night_avg_import")) or 0},
    "projected_night_import_kwh": <projected>,
    "annual_savings_kwh": <savings>
  }},
  "summary": "<2-3 sentence analysis>"
}}
"""


    print("[AI] Calling Gemini model...")
    try:
        response = model.generate_content(prompt)
        text = response.text.strip()
        print(f"[AI] Raw Response Received: {text[:200]}...")
        
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text:
            text = text.split("```")[1].split("```")[0].strip()

        res = json.loads(text)
        print(f"[AI] Successfully parsed JSON. Projected GI: {res.get('projected_grid_independence')}%")
        print(f"[AI] FULL RECOMMENDATION:\n{json.dumps(res, indent=2)}")
        print(f"[AI] {'-'*10} Recommendation Complete {'-'*10}\n")
        return res
    except Exception as e:
        print(f"[AI] ERROR in generation/parsing: {e}")
        return {
            "error": f"AI Generation failed: {str(e)}",
            "fallback": True,
            "current_grid_independence": 0,
            "projected_grid_independence": 0,
            "solar": {"status": "Unknown", "action": "Check manually", "current_kw": 0, "recommended_kw": 0, "production_gain_kwh": 0},
            "battery": {"status": "Unknown", "action": "Check manually", "current_kwh": 0, "recommended_kwh": 0, "backup_hours_gain": 0},
            "inverter": {"status": "Unknown", "action": "Check manually", "current_kw": 0, "recommended_kw": 0},
            "grid_impact": {"current_daily_import_kwh": 0, "projected_daily_import_kwh": 0, "current_night_import_kwh": 0, "projected_night_import_kwh": 0, "annual_savings_kwh": 0},
            "summary": "Unable to generate AI recommendation at this time."
        }


