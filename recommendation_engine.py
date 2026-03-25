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
    data = {"system_id": system_id}

    # 1. Postgres — use query_system.query_system() which already does the full JOIN
    try:
        pg = qs.query_system(system_id)
        if pg:
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
    except Exception as e:
        print(f"Error fetching Postgres data: {e}")

    # 2. Import-export summary CSV (if exists)
    ie_path = os.path.join(out_dir, "import-export.csv")
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
        except Exception as e:
            print(f"Error reading import-export CSV: {e}")

    # 3. Monthly dashboard data CSV (if exists)
    monthly_path = os.path.join(out_dir, f"{system_id}_dashboard-data-monthly.csv")
    if os.path.exists(monthly_path):
        try:
            df_m = pd.read_csv(monthly_path)
            data["monthly_history"] = df_m.to_dict(orient="records")
        except Exception as e:
            print(f"Error reading monthly CSV: {e}")

    # 4. BMS SOC summary CSV (if exists)
    bms_path = os.path.join(out_dir, f"{system_id}_bms_soc_summary.csv")
    if os.path.exists(bms_path):
        try:
            df_bms = pd.read_csv(bms_path)
            if not df_bms.empty:
                data["bms_health"] = df_bms.iloc[0].to_dict()
        except Exception as e:
            print(f"Error reading BMS CSV: {e}")

    # 5. Summary CSV (if exists — fallback for extra metrics)
    summary_path = os.path.join(out_dir, f"{system_id}_summary.csv")
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
        except Exception as e:
            print(f"Error reading summary CSV: {e}")

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
    cfg = load_config()
    api_key = cfg.get("secrets", {}).get("GEMINI_API_KEY")
    if not api_key:
        return {"error": "Gemini API Key missing in conf.yaml"}

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(
        'gemini-2.0-flash',
        generation_config=genai.types.GenerationConfig(temperature=0)
    )

    # Pre-calculate current grid independence from real data
    ie = system_data.get("import_export") or system_data.get("summary_metrics") or {}
    specs = system_data.get("specs") or {}
    daily_import = _safe_float(ie.get("daily_avg_import")) or 0
    total_kwh = _safe_float((system_data.get("summary_metrics") or {}).get("total_kwh"))
    pv_kw = _safe_float(specs.get("panels_capacity_kw")) or 0
    sun_hours = _safe_float((system_data.get("summary_metrics") or {}).get("sun_hours_per_day")) or 5
    daily_production = pv_kw * sun_hours * 0.8  # estimated daily solar output
    daily_load = daily_production + daily_import  # total load ≈ what solar covers + what grid covers
    current_gi = round(((daily_load - daily_import) / daily_load * 100), 1) if daily_load > 0 else 0

    prompt = f"""
As a Solar Energy Expert, analyze the following system data and provide recommendations to achieve {target_independence}% Grid Independence.
Grid independence = percentage of total load covered by Solar + Battery, minimizing grid import.

SYSTEM DATA:
{json.dumps(system_data, indent=2, default=str)}

PRE-CALCULATED (use these exact values):
- Current Grid Independence: {current_gi}%
- Estimated Daily Solar Production: {round(daily_production, 2)} kWh
- Estimated Daily Load: {round(daily_load, 2)} kWh
- Daily Grid Import: {round(daily_import, 2)} kWh


TARGET: {target_independence}% Grid Independence

Return ONLY a JSON object with this EXACT structure. Use numeric values (no units in numbers). Be precise with calculations:
{{
  "current_grid_independence": <number 0-100, estimated current %>,
  "projected_grid_independence": <number 0-100, projected % after upgrades>,
  "solar": {{
    "status": "<OK|Increase|Repair|Replace>",
    "action": "<short 3-5 word action>",
    "current_kw": <current PV capacity number>,
    "recommended_kw": <recommended PV capacity number>,
    "production_gain_kwh": <estimated daily kWh gain>
  }},
  "battery": {{
    "status": "<OK|Increase|Repair|Replace>",
    "action": "<short 3-5 word action>",
    "current_kwh": <current battery kWh number>,
    "recommended_kwh": <recommended battery kWh number>,
    "backup_hours_gain": <hours of additional backup>
  }},
  "inverter": {{
    "status": "<OK|Upgrade|Repair|Replace>",
    "action": "<short 3-5 word action>",
    "current_kw": <current inverter kW>,
    "recommended_kw": <recommended inverter kW>
  }},
  "grid_impact": {{
    "current_daily_import_kwh": <current avg daily import>,
    "projected_daily_import_kwh": <projected after upgrades>,
    "current_night_import_kwh": <current avg night import>,
    "projected_night_import_kwh": <projected night import after upgrades>,
    "annual_savings_kwh": <estimated annual kWh savings>
  }},
  "summary": "<2-3 sentence descriptive overall analysis and recommendation>"
}}

Rules:
- Calculate current grid independence from: (1 - daily_import / total_daily_load) * 100
- If current PV vs load is low, suggest increasing Solar.
- If night import is high or BMS SOC drops below 30%, suggest increasing Battery.
- If system deployed > 5 years ago, check Inverter health.
- If Net Metering is NOT activated, prioritize battery.
- Use real numbers from the data, do NOT fabricate values.
"""


    try:
        response = model.generate_content(prompt)
        text = response.text.strip()
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text:
            text = text.split("```")[1].split("```")[0].strip()

        return json.loads(text)
    except Exception as e:
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

