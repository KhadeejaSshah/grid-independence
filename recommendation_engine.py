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
    Gathers system data from Postgres (via query_system) and the
    pre-generated _summary.csv ONLY. No other CSVs are used.
    """
    print(f"\n[DEBUG] {'='*20} GATHERING DATA: {system_id} {'='*20}")
    data = {"system_id": system_id}

    # 1. Postgres (for system specs/metadata)
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
        else:
            print(f"[DEBUG] Postgres: No data found for {system_id}")
    except Exception as e:
        print(f"[DEBUG] Postgres ERROR: {e}")

    # 2. Summary CSV ONLY — the single source of truth for metrics
    summary_path = os.path.join(out_dir, f"{system_id}_summary.csv")
    print(f"[DEBUG] Checking CSV: {summary_path}")
    if os.path.exists(summary_path):
        try:
            df_s = pd.read_csv(summary_path)
            if not df_s.empty:
                row = df_s.iloc[0]
                data["summary_metrics"] = {}
                # Read ALL columns from the summary CSV dynamically
                for col in df_s.columns:
                    data["summary_metrics"][col] = _safe_float(row.get(col))
                    # Keep string values for non-numeric fields
                    if data["summary_metrics"][col] is None and row.get(col) is not None:
                        val = row.get(col)
                        if isinstance(val, str) and val.strip():
                            data["summary_metrics"][col] = val
                print(f"[DEBUG] CSV Found (summary): {len(data['summary_metrics'])} fields loaded")
                print(f"[DEBUG] Summary fields: {list(data['summary_metrics'].keys())}")
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
    Returns recommendations for ALL tiers (25%, 50%, 75%, 100%) with
    proportional scaling from the 100% tier.
    """
    print(f"\n[AI] {'='*20} STARTING RECOMMENDATION {'='*20}")
    print(f"[AI] Target Independence: {target_independence}%")

    # Guard: require summary CSV data for accurate recommendations
    if not system_data.get("summary_metrics"):
        print("[AI] ERROR: No summary CSV data available. Run oldcombineall.py first.")
        return {
            "error": "Summary CSV not found. Please run the data pipeline (oldcombineall.py) for this system first.",
            "data_missing": True,
            "current_grid_independence": 0,
            "projected_grid_independence": 0,
            "solar": {"status": "Unknown", "action": "Run pipeline first", "current_kw": 0, "recommended_kw_100": 0, "production_gain_kwh": 0},
            "battery": {"status": "Unknown", "action": "Run pipeline first", "current_kwh": 0, "recommended_kwh_100": 0, "backup_hours_gain": 0},
            "inverter": {"status": "Unknown", "action": "Run pipeline first", "current_kw": 0, "recommended_kw_100": 0},
            "grid_impact": {"current_daily_import_kwh": 0, "projected_daily_import_kwh": 0, "current_night_import_kwh": 0, "projected_night_import_kwh": 0, "annual_savings_kwh": 0},
            "summary": "Cannot generate recommendation: summary CSV data is missing. Please run oldcombineall.py for this system first.",
            "tiers": {}
        }

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

    # Pre-calculate current grid independence from summary data
    print("[AI] Running pre-calculations...")
    sm = system_data.get("summary_metrics") or {}
    specs = system_data.get("specs") or {}

    daily_import = _safe_float(sm.get("daily_avg_import")) or 0
    pv_kw = _safe_float(specs.get("panels_capacity_kw")) or 0
    sun_hours = _safe_float(sm.get("sun_hours_per_day")) or 5
    avg_total_load = _safe_float(sm.get("avg_total_load"))
    avg_night_fraction = _safe_float(sm.get("avg_night_fraction"))
    battery_kwh = _safe_float(specs.get("current_battery_kwh")) or 0
    inverter_kw = _safe_float(specs.get("inverters_capacity_kw")) or 0
    actual_system_age = _safe_float(sm.get("actual_system_age"))
    inverter_capacity = _safe_float(sm.get("inverter_capacity")) or inverter_kw

    daily_production = pv_kw * sun_hours * 0.8  # estimated daily solar output
    daily_load = daily_production + daily_import  # total load ≈ what solar covers + what grid covers
    current_gi = round(((daily_load - daily_import) / daily_load * 100), 1) if daily_load > 0 else 0
    target_met = current_gi >= target_independence

    print(f"[AI] Pre-calc values: import={daily_import}, pv_kw={pv_kw}, sun_hours={sun_hours}")
    print(f"[AI] Pre-calc results: production={daily_production}, load={daily_load}, current_gi={current_gi}%, target_met={target_met}")

    prompt = f"""
As a Solar Energy Expert, analyze the following system data and provide recommendations to achieve 100% Grid Independence.
Grid independence = percentage of total load covered by Solar + Battery, minimizing grid import.

SYSTEM DATA (from summary CSV and Postgres):
{json.dumps(system_data, indent=2, default=str)}

PRE-CALCULATED FIGURES (USE THESE):
- Current Grid Independence: {current_gi}%
- Target: {target_independence}%
- Target already met: {"YES" if target_met else "NO"}
- Estimated Daily Solar Production: {round(daily_production, 2)} kWh
- Estimated Daily Load: {round(daily_load, 2)} kWh
- Daily Grid Import: {round(daily_import, 2)} kWh
- Current PV Capacity: {pv_kw} kW
- Current Battery: {battery_kwh} kWh
- Current Inverter: {inverter_capacity} kW
- System Age (days): {actual_system_age}
- Avg Total Load: {avg_total_load}
- Avg Night Fraction: {avg_night_fraction}

CRITICAL RULES:
1. First, calculate what is needed for 100% grid independence.
2. Then I will scale the other tiers proportionally:
   - 75% tier = 75% of the ADDITIONAL capacity needed beyond current
   - 50% tier = 50% of the ADDITIONAL capacity needed beyond current
   - 25% tier = 25% of the ADDITIONAL capacity needed beyond current
3. If "Target already met" is YES, set status to "OK" and recommended equals current.
4. Do NOT confuse "Panels Capacity" ({pv_kw} kW) with "Current PV Production".

Return ONLY a valid JSON object with this structure:
{{
  "current_grid_independence": {current_gi},
  "projected_grid_independence": <number for 100% tier>,
  "solar": {{
    "status": "<OK|Increase|Repair|Replace>",
    "action": "<short 3-5 word action>",
    "current_kw": {pv_kw},
    "recommended_kw_100": <kW needed for 100% independence>,
    "production_gain_kwh": <kWh gain at 100%>
  }},
  "battery": {{
    "status": "<OK|Increase|Repair|Replace>",
    "action": "<short 3-5 word action>",
    "current_kwh": {battery_kwh},
    "recommended_kwh_100": <kWh needed for 100% independence>,
    "backup_hours_gain": <hours gain>
  }},
  "inverter": {{
    "status": "<OK|Upgrade|Repair|Replace>",
    "action": "<short 3-5 word action>",
    "current_kw": {inverter_capacity},
    "recommended_kw_100": <kW needed for 100% independence>
  }},
  "grid_impact": {{
    "current_daily_import_kwh": {daily_import},
    "projected_daily_import_kwh": <projected at 100%>,
    "current_night_import_kwh": {_safe_float(sm.get("night_avg_import")) or 0},
    "projected_night_import_kwh": <projected at 100%>,
    "annual_savings_kwh": <savings at 100%>
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

        # Now apply proportional scaling for all tiers
        res = _apply_proportional_tiers(res, pv_kw, battery_kwh, inverter_capacity, daily_import,
                                        _safe_float(sm.get("night_avg_import")) or 0)

        print(f"[AI] FULL RECOMMENDATION WITH TIERS:\n{json.dumps(res, indent=2)}")
        print(f"[AI] {'-'*10} Recommendation Complete {'-'*10}\n")
        return res
    except Exception as e:
        print(f"[AI] ERROR in generation/parsing: {e}")
        return {
            "error": f"AI Generation failed: {str(e)}",
            "fallback": True,
            "current_grid_independence": 0,
            "projected_grid_independence": 0,
            "solar": {"status": "Unknown", "action": "Check manually", "current_kw": 0, "recommended_kw_100": 0, "production_gain_kwh": 0},
            "battery": {"status": "Unknown", "action": "Check manually", "current_kwh": 0, "recommended_kwh_100": 0, "backup_hours_gain": 0},
            "inverter": {"status": "Unknown", "action": "Check manually", "current_kw": 0, "recommended_kw_100": 0},
            "grid_impact": {"current_daily_import_kwh": 0, "projected_daily_import_kwh": 0, "current_night_import_kwh": 0, "projected_night_import_kwh": 0, "annual_savings_kwh": 0},
            "summary": "Unable to generate AI recommendation at this time."
        }


def _apply_proportional_tiers(res: Dict, current_pv: float, current_batt: float,
                               current_inv: float, current_daily_import: float,
                               current_night_import: float) -> Dict:
    """
    Given the AI's 100% recommendation, compute proportional tiers for 75%, 50%, 25%.
    Additional capacity needed = recommended_100 - current.
    Each tier gets tier_pct% of that additional capacity added to current.
    """
    solar = res.get("solar", {})
    battery = res.get("battery", {})
    inverter = res.get("inverter", {})
    grid_impact = res.get("grid_impact", {})

    # Additional capacity needed for 100%
    solar_add = max(0, (solar.get("recommended_kw_100") or current_pv) - current_pv)
    batt_add = max(0, (battery.get("recommended_kwh_100") or current_batt) - current_batt)
    inv_add = max(0, (inverter.get("recommended_kw_100") or current_inv) - current_inv)

    # Import reduction at 100%
    projected_import_100 = grid_impact.get("projected_daily_import_kwh") or 0
    import_reduction = current_daily_import - projected_import_100

    projected_night_100 = grid_impact.get("projected_night_import_kwh") or 0
    night_reduction = current_night_import - projected_night_100

    annual_savings_100 = grid_impact.get("annual_savings_kwh") or 0

    tiers = {}
    for pct in [25, 50, 75, 100]:
        frac = pct / 100.0
        tier_solar = round(current_pv + solar_add * frac, 2)
        tier_batt = round(current_batt + batt_add * frac, 2)
        tier_inv = round(current_inv + inv_add * frac, 2)
        tier_import = round(current_daily_import - import_reduction * frac, 2)
        tier_night = round(current_night_import - night_reduction * frac, 2)
        tier_savings = round(annual_savings_100 * frac, 2)
        tier_production_gain = round((solar.get("production_gain_kwh") or 0) * frac, 2)
        tier_backup_gain = round((battery.get("backup_hours_gain") or 0) * frac, 2)

        tiers[f"tier_{pct}"] = {
            "target_independence": pct,
            "solar": {
                "recommended_kw": tier_solar,
                "additional_kw": round(solar_add * frac, 2),
                "production_gain_kwh": tier_production_gain,
            },
            "battery": {
                "recommended_kwh": tier_batt,
                "additional_kwh": round(batt_add * frac, 2),
                "backup_hours_gain": tier_backup_gain,
            },
            "inverter": {
                "recommended_kw": tier_inv,
                "additional_kw": round(inv_add * frac, 2),
            },
            "grid_impact": {
                "projected_daily_import_kwh": tier_import,
                "projected_night_import_kwh": tier_night,
                "annual_savings_kwh": tier_savings,
            }
        }

    res["tiers"] = tiers
    return res
