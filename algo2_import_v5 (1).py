"""
Algorithm 2 — IMPORT-DRIVEN SIZING ENGINE  (v5)
================================================
KEY FIX vs v4
─────────────────────────────────────────────────────────────────────────
For every tier the engine now works in TWO steps:

  STEP 1 — Run a 24-hour simulation with the EXISTING equipment
           (cur_pv, cur_bat_eff, cur_bat_pw).  This reveals how much
           import the existing system already offsets.

           result_existing = {day_residual, night_residual}

  STEP 2 — The remaining gaps after existing equipment:
             day_gap   = max(0,  day_target  − day_covered_by_existing)
             night_gap = max(0, night_target − night_covered_by_existing)

           day_gap   → add PV
           night_gap → add Battery

  This means 25/50/75% tiers can correctly show ZERO additions when
  the existing kit already covers the target, and will add the RIGHT
  amount when it doesn't.

INPUT (six direct import numbers — no fractions needed)
───────────────────────────────────────────────────────
  avg_daily_import_kwh      → typical day total
  avg_day_import_kwh        → daytime portion of typical day
  avg_night_import_kwh      → night-time portion of typical day
  peak_daily_import_kwh     → worst day ever seen
  peak_day_import_kwh       → daytime portion of worst day
  peak_night_import_kwh     → night-time portion of worst day
  peak_import_power_kw      → highest recorded import power (kW)
  avg_load_power_kw         → average load power (kW)

TIER MAPPING
────────────────────────────────────────────────────────────────────────
  25%  → eliminate 25% of avg day + 25% of avg night
  50%  → eliminate 50% of avg day + 50% of avg night
  75%  → eliminate 75% of avg day + 75% of avg night
 100%  → eliminate 100% of peak day + 100% of peak night

Usage
─────
    python algo2_import_v5.py --csv input_v5.csv
    python algo2_import_v5.py --csv input_v5.csv --pdf report.pdf --output-json out.json
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
from typing import Any

# ── PDF imports ───────────────────────────────────────────────────────────────
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, PageBreak,
)


# =============================================================================
# SECTION 1 — CSV LOADER
# =============================================================================

def load_csv(filepath: str) -> dict:
    raw: dict[str, str] = {}
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw[row["parameter"].strip()] = row["value"].strip()

    def flt(key: str, default: float) -> float:
        return float(raw[key]) if key in raw else default

    return {
        # ── SIX DIRECT IMPORT INPUTS ─────────────────────────────────────
        "avg_daily_import_kwh":       flt("avg_daily_import_kwh",      18.0),
        "avg_day_import_kwh":         flt("avg_day_import_kwh",        10.5),
        "avg_night_import_kwh":       flt("avg_night_import_kwh",       7.5),
        "peak_daily_import_kwh":      flt("peak_daily_import_kwh",     34.0),
        "peak_day_import_kwh":        flt("peak_day_import_kwh",       20.0),
        "peak_night_import_kwh":      flt("peak_night_import_kwh",     14.0),
        "peak_import_power_kw":       flt("peak_import_power_kw",       9.5),
        "avg_load_power_kw":          flt("avg_load_power_kw",          3.2),

        # ── EXISTING EQUIPMENT ───────────────────────────────────────────
        "current_pv_kw":              flt("current_pv_kw",              3.0),
        "current_battery_kwh":        flt("current_battery_kwh",       10.0),
        "current_battery_power_kw":   flt("current_battery_power_kw",   5.0),
        "current_inverter_kw":        flt("current_inverter_kw",        5.0),

        # ── BATTERY SOH ──────────────────────────────────────────────────
        "current_battery_soh":        flt("current_battery_soh",        0.85),
        "new_battery_eol_soh":        flt("new_battery_eol_soh",        0.80),
        "new_battery_design_years":   int(flt("new_battery_design_years", 10)),

        # ── BATTERY TECHNICAL ────────────────────────────────────────────
        "battery_dod":                flt("battery_dod",                0.85),
        "battery_efficiency":         flt("battery_efficiency",         0.90),
        "battery_safety_margin":      flt("battery_safety_margin",      1.20),

        # ── SOLAR RESOURCE & SITE ────────────────────────────────────────
        "sun_hours_per_day":          flt("sun_hours_per_day",          5.5),
        "system_loss_factor":         flt("system_loss_factor",         0.15),
        "roof_available_kw":          flt("roof_available_kw",        999.0),

        # ── INVERTER SIZING RULE ─────────────────────────────────────────
        # Required inverter = peak_import_power + total_pv (simultaneous back-feed)
        # Override this auto-calculation by setting inverter_sizing_rule = "manual"
        # and providing required_inverter_kw.
        "inverter_sizing_rule":       raw.get("inverter_sizing_rule", "auto"),
        "required_inverter_kw":       flt("required_inverter_kw",       0.0),

        # ── ALGORITHM CONTROL ────────────────────────────────────────────
        "max_iterations":             int(flt("max_iterations",          50)),
    }


# =============================================================================
# SECTION 2 — HELPERS
# =============================================================================

def _soh_projection(nominal_kwh: float, eol_soh: float,
                    design_years: int, dod: float) -> list[dict]:
    annual_deg = (1.0 - eol_soh) / max(design_years, 1)
    rows = []
    for yr in range(0, design_years + 1):
        soh = max(round(1.0 - annual_deg * yr, 4), eol_soh)
        rows.append({
            "year":       yr,
            "soh_pct":    round(soh * 100, 1),
            "usable_kwh": round(nominal_kwh * soh * dod, 2),
        })
    return rows


def _pv_profile(pv_kw_net: float, sun_h: float) -> list[float]:
    """Sinusoidal PV generation profile over 24 hours."""
    pv    = [0.0] * 24
    day_h = list(range(6, 19))
    w     = [math.sin(math.pi * (h - 6) / 12) for h in day_h]
    tw    = sum(w)
    daily = pv_kw_net * sun_h
    for i, h in enumerate(day_h):
        pv[h] = daily * w[i] / tw
    return pv


def _import_profile(day_kwh: float, night_kwh: float) -> list[float]:
    """Flat import profile split between daytime (6-18) and night-time (18-6)."""
    imp     = [0.0] * 24
    day_h   = list(range(6, 18))                         # 12 h
    night_h = list(range(18, 24)) + list(range(0, 6))   # 12 h
    for h in day_h:
        imp[h] = day_kwh / 12.0
    for h in night_h:
        imp[h] = night_kwh / 12.0
    return imp


def _simulate(import_profile: list[float], pv_profile: list[float],
              bat_kwh: float, bat_kw: float,
              dod: float, eff: float) -> dict:
    """
    Hourly simulation:
      1. PV covers import first.
      2. Surplus PV charges battery.
      3. Battery covers remaining import.
    Returns residual import split by day / night.
    """
    usable  = bat_kwh * dod
    soc     = usable * 0.5
    sq      = math.sqrt(eff)
    night_h = set(list(range(18, 24)) + list(range(0, 6)))
    res_h   = [0.0] * 24

    for h in range(24):
        need    = import_profile[h]
        gen     = pv_profile[h]
        cov_pv  = min(gen, need)
        need   -= cov_pv
        surplus = gen - cov_pv
        if surplus > 0:
            chg = min(surplus, bat_kw, (usable - soc) / max(sq, 1e-9))
            soc += max(chg, 0.0) * sq
        if need > 0:
            dis = min(need, bat_kw, soc * sq)
            soc -= max(dis, 0.0) / sq
            need -= dis
        res_h[h] = max(need, 0.0)

    night_res = sum(res_h[h] for h in range(24) if h in night_h)
    day_res   = sum(res_h[h] for h in range(24) if h not in night_h)
    return {
        "total": round(sum(res_h), 4),
        "night": round(night_res, 4),
        "day":   round(day_res, 4),
    }


# =============================================================================
# SECTION 3 — SINGLE-TIER SIZING  (gap-based, two-step)
# =============================================================================

def _size_for_tier(
    tier_day_import:   float,   # kWh the tier requires offsetting during day
    tier_night_import: float,   # kWh the tier requires offsetting at night
    cur_pv:    float,           # kW  existing PV
    cur_bat_eff: float,         # kWh existing battery effective (SOH-derated)
    cur_bat_np:  float,         # kWh existing battery nameplate
    cur_bat_pw:  float,         # kW  existing battery power
    dod: float, eff: float,
    bat_margin: float,
    sun_h: float, loss_f: float,
    roof_lim: float,
    max_iter: int,
    eol_soh: float, des_yrs: int,
) -> dict:
    """
    Two-step gap-based sizing for ONE tier.

    STEP 1: Simulate with existing equipment against the tier's import profile.
            → residual_existing = what existing kit CANNOT cover.

    STEP 2: Compute gaps:
              day_gap   = tier_day_import  − (tier_day_import  − existing_day_residual)
                        = existing_day_residual   (capped to tier_day_import)
              night_gap = existing_night_residual  (capped to tier_night_import)

            day_gap   → add PV
            night_gap → add Battery

    Then run an optimisation loop to close any remaining residual.
    """
    warnings: list[str] = []

    # ── STEP 1: simulate existing equipment ──────────────────────────────
    imp_prof_tier = _import_profile(tier_day_import, tier_night_import)
    pv_prof_exist = _pv_profile(cur_pv * (1 - loss_f), sun_h)

    sim_exist = _simulate(
        imp_prof_tier, pv_prof_exist,
        cur_bat_eff, cur_bat_pw, dod, eff
    )
    # Gaps = what existing equipment leaves uncovered for THIS tier
    day_gap   = max(0.0, sim_exist["day"])
    night_gap = max(0.0, sim_exist["night"])

    # ── STEP 2: size PV for day gap ──────────────────────────────────────
    # NOTE: day_gap already came from a simulation that used existing PV.
    # Do NOT subtract cur_pv — the gap is already net of existing PV.
    # Only apply charging-loss overhead when there is an actual gap to close.
    if day_gap > 0.01:
        charging_loss  = day_gap * (1 - eff) * 1.1
        pv_output_need = day_gap + charging_loss
        pv_add_kw      = max(0.0, pv_output_need / max(sun_h, 0.1) / (1 - loss_f))
    else:
        pv_add_kw = 0.0

    roof_constrained = False
    if pv_add_kw > roof_lim - cur_pv:
        warnings.append(
            f"PV addition {pv_add_kw:.2f} kW exceeds roof headroom "
            f"{max(0.0, roof_lim - cur_pv):.1f} kW — capped."
        )
        pv_add_kw = max(0.0, roof_lim - cur_pv)
        roof_constrained = True

    total_pv_kw = cur_pv + pv_add_kw

    # ── STEP 2: size Battery for night gap ───────────────────────────────
    usable_needed    = night_gap * bat_margin
    gross_needed     = usable_needed / max(dod, 1e-9)
    nominal_add      = max(0.0, gross_needed - cur_bat_eff)
    eol_factor       = round(1.0 / max(eol_soh, 0.01), 4)
    nameplate_order  = nominal_add * eol_factor

    bat_pw_floor  = (night_gap * bat_margin) / 12.0
    bat_pw_add    = max(0.0, bat_pw_floor - cur_bat_pw)

    # Total system figures (existing nameplate + new nameplate for display)
    total_bat_sys_np = cur_bat_np + nameplate_order
    total_bat_eff    = cur_bat_eff + nominal_add   # effective usable
    total_bat_pw     = cur_bat_pw + bat_pw_add

    soh_proj = _soh_projection(nominal_add, eol_soh, des_yrs, dod)

    # ── OPTIMISATION LOOP ─────────────────────────────────────────────────
    # Run simulation with newly sized equipment and iterate until residual = 0
    bat_sim  = total_bat_eff   # effective kWh for simulation
    bpw_sim  = total_bat_pw
    pv_sim   = total_pv_kw

    pv_prof  = _pv_profile(pv_sim * (1 - loss_f), sun_h)
    sim      = _simulate(imp_prof_tier, pv_prof, bat_sim, bpw_sim, dod, eff)

    INC_PV  = 0.5
    INC_BAT = 1.0
    iteration = 0
    opt_log   = []

    while sim["total"] > 0.05 and iteration < max_iter:
        iteration += 1
        if sim["night"] > 0.01:
            bat_sim  += INC_BAT
            bpw_sim  += INC_BAT * 0.25
            action = f"Night {sim['night']:.3f} kWh → +{INC_BAT} kWh bat"
        elif sim["day"] > 0.01:
            if pv_sim + INC_PV <= roof_lim:
                pv_add_kw += INC_PV
                pv_sim    += INC_PV
                action = f"Day {sim['day']:.3f} kWh → +{INC_PV} kW PV"
            else:
                bat_sim += INC_BAT
                bpw_sim += INC_BAT * 0.25
                action  = f"Day, roof limit → +{INC_BAT} kWh bat"
                warnings.append(f"Iter {iteration}: roof limit hit, adding battery instead.")
        else:
            if pv_sim + INC_PV <= roof_lim:
                pv_add_kw += INC_PV
                pv_sim    += INC_PV
                action = f"Residual → +{INC_PV} kW PV"
            else:
                bat_sim += INC_BAT
                bpw_sim += INC_BAT * 0.25
                action  = f"Residual, roof limit → +{INC_BAT} kWh bat"

        pv_prof = _pv_profile(pv_sim * (1 - loss_f), sun_h)
        sim = _simulate(imp_prof_tier, pv_prof, bat_sim, bpw_sim, dod, eff)
        opt_log.append({"iter": iteration, "action": action, "residual": sim["total"]})

    # ── Compute final additions relative to existing ──────────────────────
    final_pv_add      = pv_sim - cur_pv
    final_bat_eff_add = bat_sim - cur_bat_eff          # effective kWh added
    final_bat_np_add  = final_bat_eff_add * eol_factor  # nameplate to order
    final_bat_pw_add  = bpw_sim - cur_bat_pw

    total_bat_np_final = cur_bat_np + final_bat_np_add

    tier_total  = tier_day_import + tier_night_import
    reliability = round(
        min(100.0, (1 - sim["total"] / max(tier_total, 0.001)) * 100), 1
    )

    return {
        # Targets
        "tier_day_import_kwh":          round(tier_day_import, 3),
        "tier_night_import_kwh":        round(tier_night_import, 3),
        "tier_total_import_kwh":        round(tier_total, 3),
        # Existing coverage
        "existing_day_residual_kwh":    round(sim_exist["day"], 4),
        "existing_night_residual_kwh":  round(sim_exist["night"], 4),
        "existing_total_residual_kwh":  round(sim_exist["total"], 4),
        # Gaps
        "day_gap_kwh":                  round(day_gap, 4),
        "night_gap_kwh":                round(night_gap, 4),
        # PV
        "pv_addition_kw":               round(final_pv_add, 2),
        "total_pv_kw":                  round(pv_sim, 2),
        "roof_constrained":             roof_constrained,
        # Battery energy
        "usable_energy_needed_kwh":     round(usable_needed, 2),
        "nominal_addition_kwh":         round(final_bat_eff_add, 2),
        "nameplate_to_order_kwh":       round(final_bat_np_add, 2),
        "total_battery_nameplate_kwh":  round(total_bat_np_final, 2),
        "total_battery_effective_kwh":  round(bat_sim, 2),
        # Battery power
        "battery_power_addition_kw":    round(final_bat_pw_add, 2),
        "total_battery_power_kw":       round(bpw_sim, 2),
        # SOH
        "eol_upsize_factor":            eol_factor,
        "soh_projection":               soh_proj,
        # Performance
        "reliability_pct":              reliability,
        "residual_import_kwh":          round(sim["total"], 4),
        "residual_night_kwh":           round(sim["night"], 4),
        "residual_day_kwh":             round(sim["day"], 4),
        "optimisation_iterations":      iteration,
        "warnings":                     warnings,
    }


# =============================================================================
# SECTION 4 — INVERTER SIZING
# =============================================================================

def _required_inverter_kw(peak_import_power: float, total_pv_kw: float,
                           rule: str, manual_kw: float) -> float:
    """
    Auto rule: inverter must handle peak import power AND peak PV export
    simultaneously. Required = peak_import_power + total_pv (conservative).
    Manual rule: use the provided value.
    """
    if rule == "manual" and manual_kw > 0:
        return manual_kw
    return round(peak_import_power + total_pv_kw, 1)


# =============================================================================
# SECTION 5 — MAIN ENGINE
# =============================================================================

def import_driven_sizing(cd: dict) -> dict:
    # ── Input extraction ─────────────────────────────────────────────────
    avg_day_imp   = cd["avg_day_import_kwh"]
    avg_ngt_imp   = cd["avg_night_import_kwh"]
    avg_tot_imp   = cd["avg_daily_import_kwh"]
    pk_day_imp    = cd["peak_day_import_kwh"]
    pk_ngt_imp    = cd["peak_night_import_kwh"]
    pk_tot_imp    = cd["peak_daily_import_kwh"]
    pk_pwr        = cd["peak_import_power_kw"]
    avg_pwr       = cd["avg_load_power_kw"]

    cur_pv        = cd["current_pv_kw"]
    cur_bat_np    = cd["current_battery_kwh"]
    cur_bat_pw    = cd["current_battery_power_kw"]
    cur_inv       = cd["current_inverter_kw"]
    cur_soh       = cd["current_battery_soh"]
    eol_soh       = cd["new_battery_eol_soh"]
    des_yrs       = cd["new_battery_design_years"]
    dod           = cd["battery_dod"]
    eff           = cd["battery_efficiency"]
    bat_margin    = cd["battery_safety_margin"]
    sun_h         = cd["sun_hours_per_day"]
    loss_f        = cd["system_loss_factor"]
    roof_lim      = cd["roof_available_kw"]
    max_iter      = cd["max_iterations"]
    inv_rule      = cd["inverter_sizing_rule"]
    inv_manual    = cd["required_inverter_kw"]

    # SOH-derate existing battery
    cur_bat_eff   = cur_bat_np * cur_soh

    soh_warnings = []
    if cur_soh < 1.0:
        soh_warnings.append(
            f"Existing battery derated: nameplate {cur_bat_np:.2f} kWh × "
            f"SOH {cur_soh*100:.0f}% = {cur_bat_eff:.2f} kWh effective "
            f"(lost {cur_bat_np - cur_bat_eff:.2f} kWh)."
        )

    # ── Compute import surplus (what existing kit leaves uncovered) ───────
    full_imp_prof = _import_profile(avg_day_imp, avg_ngt_imp)
    exist_pv_prof = _pv_profile(cur_pv * (1 - loss_f), sun_h)
    sim_existing  = _simulate(full_imp_prof, exist_pv_prof,
                              cur_bat_eff, cur_bat_pw, dod, eff)
    surplus_day   = max(0.0, sim_existing["day"])
    surplus_night = max(0.0, sim_existing["night"])

    # ── Tier definitions: (day_target, night_target) ─────────────────────
    #   25/50/75% → fractions of import SURPLUS (what existing kit can't cover)
    #   100%      → full peak day + peak night
    tier_defs = {
        25:  (surplus_day * 0.25, surplus_night * 0.25),
        50:  (surplus_day * 0.50, surplus_night * 0.50),
        75:  (surplus_day * 0.75, surplus_night * 0.75),
        100: (pk_day_imp,          pk_ngt_imp),
    }

    tiers = {}
    for pct, (t_day, t_ngt) in tier_defs.items():
        # For 25/50/75% the targets ARE the surplus (already net of existing
        # equipment), so we pass zero existing equipment to avoid double-
        # counting.  The 100% tier targets raw peak import, so it still
        # needs the full existing-equipment simulation inside _size_for_tier.
        if pct < 100:
            eff_pv  = 0.0
            eff_bat = 0.0
            eff_np  = 0.0
            eff_pw  = 0.0
        else:
            eff_pv  = cur_pv
            eff_bat = cur_bat_eff
            eff_np  = cur_bat_np
            eff_pw  = cur_bat_pw

        tiers[pct] = _size_for_tier(
            tier_day_import   = t_day,
            tier_night_import = t_ngt,
            cur_pv            = eff_pv,
            cur_bat_eff       = eff_bat,
            cur_bat_np        = eff_np,
            cur_bat_pw        = eff_pw,
            dod               = dod,
            eff               = eff,
            bat_margin        = bat_margin,
            sun_h             = sun_h,
            loss_f            = loss_f,
            roof_lim          = roof_lim,
            max_iter          = max_iter,
            eol_soh           = eol_soh,
            des_yrs           = des_yrs,
        )
        # For surplus-based tiers, add back existing equipment to totals
        if pct < 100:
            tiers[pct]["total_pv_kw"]                = round(cur_pv + tiers[pct]["pv_addition_kw"], 2)
            tiers[pct]["total_battery_nameplate_kwh"] = round(cur_bat_np + tiers[pct]["nameplate_to_order_kwh"], 2)
            tiers[pct]["total_battery_effective_kwh"] = round(cur_bat_eff + tiers[pct]["nominal_addition_kwh"], 2)
            tiers[pct]["total_battery_power_kw"]      = round(cur_bat_pw + tiers[pct]["battery_power_addition_kw"], 2)

    # ── Inverter sizing (per-tier, based on tier total PV) ───────────────
    for pct in [25, 50, 75, 100]:
        t = tiers[pct]
        req_inv = _required_inverter_kw(pk_pwr, t["total_pv_kw"], inv_rule, inv_manual)
        inv_add = max(0.0, req_inv - cur_inv)
        t["required_inverter_kw"]   = req_inv
        t["inverter_addition_kw"]   = round(inv_add, 1)
        t["inverter_upgrade_needed"] = inv_add > 0.01
        if inv_add > 0.01:
            t["warnings"].append(
                f"Inverter upgrade required: existing {cur_inv:.1f} kW < "
                f"required {req_inv:.1f} kW. Add {inv_add:.1f} kW inverter capacity."
            )

    return {
        "inputs": {
            "avg_daily_import_kwh":       avg_tot_imp,
            "avg_day_import_kwh":         avg_day_imp,
            "avg_night_import_kwh":       avg_ngt_imp,
            "peak_daily_import_kwh":      pk_tot_imp,
            "peak_day_import_kwh":        pk_day_imp,
            "peak_night_import_kwh":      pk_ngt_imp,
            "peak_import_power_kw":       pk_pwr,
            "avg_load_power_kw":          avg_pwr,
            "current_pv_kw":              cur_pv,
            "current_battery_nameplate_kwh": cur_bat_np,
            "current_battery_effective_kwh": round(cur_bat_eff, 2),
            "current_battery_soh_pct":    round(cur_soh * 100, 1),
            "current_battery_power_kw":   cur_bat_pw,
            "current_inverter_kw":        cur_inv,
            "battery_dod":                dod,
            "battery_efficiency":         eff,
            "battery_safety_margin":      bat_margin,
            "new_battery_eol_soh_pct":    round(eol_soh * 100, 1),
            "new_battery_design_years":   des_yrs,
            "sun_hours_per_day":          sun_h,
            "system_loss_factor":         loss_f,
            "roof_available_kw":          roof_lim,
        },
        "soh_existing": {
            "nameplate_kwh":  round(cur_bat_np, 2),
            "soh_pct":        round(cur_soh * 100, 1),
            "effective_kwh":  round(cur_bat_eff, 2),
            "lost_kwh":       round(cur_bat_np - cur_bat_eff, 2),
        },
        "tiers":    tiers,
        "warnings": soh_warnings,
    }


# =============================================================================
# SECTION 6 — CONSOLE PRINT
# =============================================================================

def print_result(result: dict) -> None:
    inp  = result["inputs"]
    soh  = result["soh_existing"]

    W = 72
    print(f"\n{'='*W}")
    print("  IMPORT-DRIVEN SIZING  v5  —  Grid Independence Report")
    print(f"{'='*W}")

    print(f"\n── SIX IMPORT INPUTS {'─'*51}")
    print(f"  Avg daily import           : {inp['avg_daily_import_kwh']:.2f} kWh")
    print(f"  Avg day import             : {inp['avg_day_import_kwh']:.2f} kWh")
    print(f"  Avg night import           : {inp['avg_night_import_kwh']:.2f} kWh")
    print(f"  Peak daily import          : {inp['peak_daily_import_kwh']:.2f} kWh")
    print(f"  Peak day import            : {inp['peak_day_import_kwh']:.2f} kWh")
    print(f"  Peak night import          : {inp['peak_night_import_kwh']:.2f} kWh")
    print(f"  Peak import power          : {inp['peak_import_power_kw']:.2f} kW")
    print(f"  Avg load power             : {inp['avg_load_power_kw']:.2f} kW")

    print(f"\n── EXISTING EQUIPMENT {'─'*50}")
    print(f"  PV                         : {inp['current_pv_kw']:.2f} kW")
    print(f"  Battery nameplate          : {soh['nameplate_kwh']:.2f} kWh"
          f"  (SOH {soh['soh_pct']:.0f}% → {soh['effective_kwh']:.2f} kWh effective)")
    print(f"  Battery power              : {inp['current_battery_power_kw']:.2f} kW")
    print(f"  Inverter                   : {inp['current_inverter_kw']:.2f} kW")

    labels = {
        25:  "25%  (quarter independence)",
        50:  "50%  (half independence)",
        75:  "75%  (three-quarter independence)",
        100: "100% (zero grid import)",
    }

    for pct in [25, 50, 75, 100]:
        t = result["tiers"][pct]
        lbl = labels[pct]
        print(f"\n── TIER {lbl} {'─'*(39 - len(lbl))}")
        print(f"  Day target  / Night target : "
              f"{t['tier_day_import_kwh']:.3f} kWh  /  {t['tier_night_import_kwh']:.3f} kWh")
        print(f"  Existing covers (day/night): "
              f"{t['tier_day_import_kwh'] - t['day_gap_kwh']:.3f} kWh  /  "
              f"{t['tier_night_import_kwh'] - t['night_gap_kwh']:.3f} kWh")
        print(f"  Gap (day    → PV needed)   : {t['day_gap_kwh']:.4f} kWh")
        print(f"  Gap (night  → Bat needed)  : {t['night_gap_kwh']:.4f} kWh")
        print(f"  PV addition                : {t['pv_addition_kw']:.2f} kW"
              f"  →  total {t['total_pv_kw']:.2f} kW")
        print(f"  Battery to order (nameplate): {t['nameplate_to_order_kwh']:.2f} kWh"
              f"  (×{t['eol_upsize_factor']:.3f} EOL)")
        print(f"  Total battery in system    : {t['total_battery_nameplate_kwh']:.2f} kWh"
              f"  (nameplate)")
        print(f"  Battery power addition     : {t['battery_power_addition_kw']:.2f} kW"
              f"  →  total {t['total_battery_power_kw']:.2f} kW")
        inv_flag = "  ⚠ UPGRADE NEEDED" if t["inverter_upgrade_needed"] else "  ✓ OK"
        print(f"  Required inverter          : {t['required_inverter_kw']:.1f} kW"
              f"{inv_flag}"
              + (f"  +{t['inverter_addition_kw']:.1f} kW" if t["inverter_upgrade_needed"] else ""))
        print(f"  Reliability achieved       : {t['reliability_pct']:.1f}%")
        print(f"  Residual import            : {t['residual_import_kwh']:.4f} kWh/day")
        if t["roof_constrained"]:
            print(f"  !! ROOF CONSTRAINED — PV capped at {inp['roof_available_kw']:.1f} kW")
        for w in t["warnings"]:
            print(f"  !  {w}")

    if result["warnings"]:
        print(f"\n── GLOBAL WARNINGS {'─'*53}")
        for w in result["warnings"]:
            print(f"  !  {w}")

    print(f"\n{'='*W}\n")


# =============================================================================
# SECTION 7 — PDF REPORT
# =============================================================================

C_DARK   = colors.HexColor("#1a2e44")
C_ACCENT = colors.HexColor("#f0a500")
C_LIGHT  = colors.HexColor("#e8f0fe")
C_MID    = colors.HexColor("#2e5f8a")
C_WHITE  = colors.white
C_GREY   = colors.HexColor("#f5f5f5")
C_RED    = colors.HexColor("#c0392b")
C_GREEN  = colors.HexColor("#27ae60")

TIER_COLOURS = {
    25:  colors.HexColor("#d4e6f1"),
    50:  colors.HexColor("#a9cce3"),
    75:  colors.HexColor("#5dade2"),
    100: colors.HexColor("#1a5276"),
}
TIER_TEXT = {25: C_DARK, 50: C_DARK, 75: C_WHITE, 100: C_WHITE}
TIER_LABELS = {
    25:  "25% Grid Independence",
    50:  "50% Grid Independence",
    75:  "75% Grid Independence",
    100: "100% Grid Independence  (Zero Import)",
}


def _styles():
    base = getSampleStyleSheet()
    def s(name, **kw):
        return ParagraphStyle(name, parent=base["Normal"], **kw)
    return {
        "cover_title": s("cover_title", fontSize=28, textColor=C_WHITE,
                          leading=34, spaceAfter=6, alignment=TA_CENTER,
                          fontName="Helvetica-Bold"),
        "cover_sub":   s("cover_sub", fontSize=13, textColor=C_ACCENT,
                          leading=18, alignment=TA_CENTER, fontName="Helvetica"),
        "section":     s("section", fontSize=13, textColor=C_WHITE,
                          leading=18, fontName="Helvetica-Bold",
                          backColor=C_DARK, spaceAfter=4, spaceBefore=10),
        "tier_head":   s("tier_head", fontSize=12, textColor=C_WHITE,
                          leading=16, fontName="Helvetica-Bold",
                          spaceAfter=4, spaceBefore=8),
        "body":        s("body", fontSize=9.5, textColor=C_DARK,
                          leading=14, fontName="Helvetica"),
        "body_bold":   s("body_bold", fontSize=9.5, textColor=C_DARK,
                          leading=14, fontName="Helvetica-Bold"),
        "warn":        s("warn", fontSize=9, textColor=C_RED,
                          leading=13, fontName="Helvetica-Oblique"),
        "small":       s("small", fontSize=8.5, textColor=colors.grey,
                          leading=12, fontName="Helvetica"),
        "table_hdr":   s("table_hdr", fontSize=9, textColor=C_WHITE,
                          alignment=TA_CENTER, fontName="Helvetica-Bold"),
        "table_cell":  s("table_cell", fontSize=9, textColor=C_DARK,
                          alignment=TA_CENTER, fontName="Helvetica"),
        "table_cell_l":s("table_cell_l", fontSize=9, textColor=C_DARK,
                          alignment=TA_LEFT, fontName="Helvetica"),
    }


def _table_style(header_bg=C_DARK, row_alt=C_GREY):
    return TableStyle([
        ("BACKGROUND",    (0, 0), (-1, 0),  header_bg),
        ("TEXTCOLOR",     (0, 0), (-1, 0),  C_WHITE),
        ("FONTNAME",      (0, 0), (-1, 0),  "Helvetica-Bold"),
        ("FONTSIZE",      (0, 0), (-1, -1), 9),
        ("ALIGN",         (0, 0), (-1, -1), "CENTER"),
        ("ALIGN",         (0, 1), (0, -1),  "LEFT"),
        ("ROWBACKGROUNDS",(0, 1), (-1, -1), [C_WHITE, row_alt]),
        ("GRID",          (0, 0), (-1, -1), 0.3, colors.HexColor("#cccccc")),
        ("TOPPADDING",    (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING",   (0, 0), (-1, -1), 6),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 6),
        ("ROUNDEDCORNERS", [3]),
    ])


def _section_header(text: str, St: dict):
    d = [[Paragraph(f"  {text}", St["section"])]]
    t = Table(d, colWidths=[A4[0] - 4.0 * cm])
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), C_DARK),
        ("TOPPADDING",    (0, 0), (-1, -1), 7),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
        ("ROUNDEDCORNERS", [4]),
    ]))
    return t


def generate_pdf(result: dict, pdf_path: str) -> None:
    inp  = result["inputs"]
    soh  = result["soh_existing"]
    St   = _styles()
    W, H = A4
    margin = 2.0 * cm
    CW = W - 2 * margin

    doc = SimpleDocTemplate(
        pdf_path, pagesize=A4,
        leftMargin=margin, rightMargin=margin,
        topMargin=margin, bottomMargin=margin,
    )
    story = []

    # ── COVER ─────────────────────────────────────────────────────────────
    cover_data = [[Paragraph("Solar System Sizing Report", St["cover_title"])]]
    cover_tbl  = Table(cover_data, colWidths=[CW])
    cover_tbl.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), C_DARK),
        ("TOPPADDING",    (0, 0), (-1, -1), 28),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 14),
        ("ROUNDEDCORNERS", [6]),
    ]))
    story.append(cover_tbl)
    story.append(Spacer(1, 0.25 * cm))
    sub_data = [[Paragraph("Import-Driven Grid Independence Analysis  (v5)", St["cover_sub"])]]
    sub_tbl  = Table(sub_data, colWidths=[CW])
    sub_tbl.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), C_MID),
        ("TOPPADDING",    (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
        ("ROUNDEDCORNERS", [4]),
    ]))
    story.append(sub_tbl)
    story.append(Spacer(1, 0.5 * cm))

    # ── SECTION 1: INPUT DATA ─────────────────────────────────────────────
    story.append(_section_header("1. Input Import Data & Existing Equipment", St))
    story.append(Spacer(1, 0.2 * cm))

    inp_rows = [
        [Paragraph("Parameter", St["table_hdr"]),
         Paragraph("Value", St["table_hdr"]),
         Paragraph("Notes", St["table_hdr"])],
        ["Avg daily import",      f"{inp['avg_daily_import_kwh']:.2f} kWh",  "Typical day total"],
        ["Avg day import",        f"{inp['avg_day_import_kwh']:.2f} kWh",    "Daytime share of typical day"],
        ["Avg night import",      f"{inp['avg_night_import_kwh']:.2f} kWh",  "Night-time share of typical day"],
        ["Peak daily import",     f"{inp['peak_daily_import_kwh']:.2f} kWh", "Worst day ever recorded"],
        ["Peak day import",       f"{inp['peak_day_import_kwh']:.2f} kWh",   "Daytime share of worst day"],
        ["Peak night import",     f"{inp['peak_night_import_kwh']:.2f} kWh", "Night-time share of worst day"],
        ["Peak import power",     f"{inp['peak_import_power_kw']:.2f} kW",   "Drives inverter sizing"],
        ["Avg load power",        f"{inp['avg_load_power_kw']:.2f} kW",      "Average consumption rate"],
        ["Current PV",            f"{inp['current_pv_kw']:.2f} kW",          "Existing solar panels"],
        ["Current battery (np)",  f"{inp['current_battery_nameplate_kwh']:.2f} kWh",
         f"SOH {inp['current_battery_soh_pct']:.0f}% → eff. {inp['current_battery_effective_kwh']:.2f} kWh"],
        ["Current battery power", f"{inp['current_battery_power_kw']:.2f} kW", ""],
        ["Current inverter",      f"{inp['current_inverter_kw']:.2f} kW",    ""],
        ["Battery DoD",           f"{inp['battery_dod']*100:.0f}%",           "Usable fraction"],
        ["Battery efficiency RT", f"{inp['battery_efficiency']*100:.0f}%",    "Round-trip"],
        ["Battery safety margin", f"{inp['battery_safety_margin']:.2f}×",    "Night load oversizing"],
        ["New battery EOL SOH",   f"{inp['new_battery_eol_soh_pct']:.0f}%",  f"At year {inp['new_battery_design_years']}"],
        ["Sun hours / day",       f"{inp['sun_hours_per_day']:.1f} h",        "Peak sun hours"],
        ["System loss factor",    f"{inp['system_loss_factor']*100:.0f}%",    "Soiling, wiring, temp"],
        ["Roof limit",            f"{inp['roof_available_kw']:.1f} kW",       "Max installable PV"],
    ]
    inp_tbl = Table(inp_rows, colWidths=[CW*0.38, CW*0.22, CW*0.40])
    inp_tbl.setStyle(_table_style())
    story.append(inp_tbl)

    # ── SECTION 2: EXISTING BATTERY SOH ──────────────────────────────────
    story.append(Spacer(1, 0.4 * cm))
    story.append(_section_header("2. Existing Battery — State of Health", St))
    story.append(Spacer(1, 0.2 * cm))
    soh_txt = (
        f"Existing battery nameplate: <b>{soh['nameplate_kwh']:.2f} kWh</b>. "
        f"At SOH <b>{soh['soh_pct']:.1f}%</b> the real usable capacity today is "
        f"<b>{soh['effective_kwh']:.2f} kWh</b> — a reduction of "
        f"<b>{soh['lost_kwh']:.2f} kWh</b>. The engine credits only this "
        f"effective figure when computing how much new capacity is needed."
    )
    story.append(Paragraph(soh_txt, St["body"]))
    story.append(Spacer(1, 0.3 * cm))
    for w in result["warnings"]:
        story.append(Paragraph(f"⚠  {w}", St["warn"]))

    # ── SECTION 3: TIER COMPARISON TABLE ─────────────────────────────────
    story.append(Spacer(1, 0.3 * cm))
    story.append(_section_header("3. Sizing Summary — All Independence Tiers", St))
    story.append(Spacer(1, 0.2 * cm))

    def cv(pct, key): return result["tiers"][pct][key]

    comp_hdr = [Paragraph(x, St["table_hdr"]) for x in
                ["Metric", "25%", "50%", "75%", "100%"]]

    def frow(label, key, fmt="{:.2f}"):
        return [label] + [fmt.format(cv(p, key)) for p in [25, 50, 75, 100]]

    comp_rows = [
        comp_hdr,
        ["Day target (kWh)"]         + [f"{cv(p,'tier_day_import_kwh'):.3f}"   for p in [25,50,75,100]],
        ["Night target (kWh)"]       + [f"{cv(p,'tier_night_import_kwh'):.3f}" for p in [25,50,75,100]],
        ["Day gap → PV (kWh)"]       + [f"{cv(p,'day_gap_kwh'):.4f}"           for p in [25,50,75,100]],
        ["Night gap → Bat (kWh)"]    + [f"{cv(p,'night_gap_kwh'):.4f}"         for p in [25,50,75,100]],
        ["PV addition (kW)"]         + [f"{cv(p,'pv_addition_kw'):.2f}"        for p in [25,50,75,100]],
        ["Total PV (kW)"]            + [f"{cv(p,'total_pv_kw'):.2f}"           for p in [25,50,75,100]],
        ["Battery to order (kWh np)"]+ [f"{cv(p,'nameplate_to_order_kwh'):.2f}"for p in [25,50,75,100]],
        ["Total bat nameplate (kWh)"]+ [f"{cv(p,'total_battery_nameplate_kwh'):.2f}" for p in [25,50,75,100]],
        ["Bat power add. (kW)"]      + [f"{cv(p,'battery_power_addition_kw'):.2f}"   for p in [25,50,75,100]],
        ["Total bat power (kW)"]     + [f"{cv(p,'total_battery_power_kw'):.2f}"       for p in [25,50,75,100]],
        ["Req. inverter (kW)"]       + [f"{cv(p,'required_inverter_kw'):.1f}"         for p in [25,50,75,100]],
        ["EOL upsize factor"]        + [f"×{cv(p,'eol_upsize_factor'):.3f}"           for p in [25,50,75,100]],
        ["Reliability (%)"]          + [f"{cv(p,'reliability_pct'):.1f}%"             for p in [25,50,75,100]],
        ["Residual import (kWh/day)"]+ [f"{cv(p,'residual_import_kwh'):.4f}"          for p in [25,50,75,100]],
    ]

    comp_tbl = Table(comp_rows, colWidths=[CW*0.36, CW*0.16, CW*0.16, CW*0.16, CW*0.16])
    ts = _table_style()
    for ci, pct in enumerate([25, 50, 75, 100], start=1):
        ts.add("BACKGROUND", (ci, 0), (ci, 0), TIER_COLOURS[pct])
        ts.add("TEXTCOLOR",  (ci, 0), (ci, 0), TIER_TEXT[pct])
    comp_tbl.setStyle(ts)
    story.append(comp_tbl)
    story.append(PageBreak())

    # ── SECTION 4: PER-TIER DETAIL ────────────────────────────────────────
    story.append(_section_header("4. Detailed Tier Analysis", St))

    for pct in [25, 50, 75, 100]:
        t = result["tiers"][pct]

        story.append(Spacer(1, 0.35 * cm))
        banner_data = [[Paragraph(TIER_LABELS[pct], St["tier_head"])]]
        banner = Table(banner_data, colWidths=[CW])
        banner.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, -1), TIER_COLOURS[pct]),
            ("TEXTCOLOR",     (0, 0), (-1, -1), TIER_TEXT[pct]),
            ("TOPPADDING",    (0, 0), (-1, -1), 8),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
            ("LEFTPADDING",   (0, 0), (-1, -1), 12),
            ("ROUNDEDCORNERS", [4]),
        ]))
        story.append(banner)
        story.append(Spacer(1, 0.2 * cm))

        covered_day = t["tier_day_import_kwh"] - t["day_gap_kwh"]
        covered_ngt = t["tier_night_import_kwh"] - t["night_gap_kwh"]

        if pct < 100:
            expl = (
                f"At <b>{pct}% grid independence</b>, the system must offset "
                f"<b>{t['tier_day_import_kwh']:.3f} kWh</b> of daytime import and "
                f"<b>{t['tier_night_import_kwh']:.3f} kWh</b> of night-time import. "
                f"The existing equipment already covers "
                f"<b>{covered_day:.3f} kWh</b> (day) and "
                f"<b>{covered_ngt:.3f} kWh</b> (night). "
                f"Remaining gaps: <b>{t['day_gap_kwh']:.4f} kWh</b> day → "
                f"<b>{t['pv_addition_kw']:.2f} kW PV added</b>; "
                f"<b>{t['night_gap_kwh']:.4f} kWh</b> night → "
                f"<b>{t['nameplate_to_order_kwh']:.2f} kWh battery ordered</b> "
                f"(×{t['eol_upsize_factor']:.3f} EOL upsize). "
                f"Simulation reliability: <b>{t['reliability_pct']:.1f}%</b>."
            )
        else:
            expl = (
                f"<b>100% grid independence</b> targets the worst recorded day: "
                f"<b>{t['tier_day_import_kwh']:.3f} kWh</b> day + "
                f"<b>{t['tier_night_import_kwh']:.3f} kWh</b> night. "
                f"Existing equipment covers "
                f"<b>{covered_day:.3f} kWh</b> (day) / <b>{covered_ngt:.3f} kWh</b> (night). "
                f"Gaps: <b>{t['day_gap_kwh']:.4f} kWh</b> day → "
                f"<b>{t['pv_addition_kw']:.2f} kW PV</b>; "
                f"<b>{t['night_gap_kwh']:.4f} kWh</b> night → "
                f"<b>{t['nameplate_to_order_kwh']:.2f} kWh nameplate battery</b>. "
                f"Residual: <b>{t['residual_import_kwh']:.4f} kWh/day</b>."
            )
        story.append(Paragraph(expl, St["body"]))
        story.append(Spacer(1, 0.2 * cm))

        detail_rows = [
            [Paragraph("Parameter", St["table_hdr"]),
             Paragraph("Value", St["table_hdr"]),
             Paragraph("Notes", St["table_hdr"])],
            ["Day target (tier)",        f"{t['tier_day_import_kwh']:.3f} kWh",  "Import to offset during day"],
            ["Night target (tier)",      f"{t['tier_night_import_kwh']:.3f} kWh","Import to offset at night"],
            ["Existing covers — day",    f"{covered_day:.3f} kWh",               "Covered by cur. PV + bat"],
            ["Existing covers — night",  f"{covered_ngt:.3f} kWh",               "Covered by cur. bat"],
            ["Day GAP → PV",             f"{t['day_gap_kwh']:.4f} kWh",          "Uncovered daytime import"],
            ["Night GAP → Battery",      f"{t['night_gap_kwh']:.4f} kWh",        "Uncovered night-time import"],
            ["Usable battery needed",    f"{t['usable_energy_needed_kwh']:.3f} kWh",
             f"Night gap × {inp['battery_safety_margin']:.2f}× margin"],
            ["Battery nominal addition", f"{t['nominal_addition_kwh']:.3f} kWh", "Effective kWh added"],
            ["Battery nameplate to ORDER",f"{t['nameplate_to_order_kwh']:.2f} kWh",
             f"Nominal ÷ EOL SOH {inp['new_battery_eol_soh_pct']:.0f}%"],
            ["Total battery (nameplate)", f"{t['total_battery_nameplate_kwh']:.2f} kWh","Existing np + ordered np"],
            ["Total battery (effective)", f"{t['total_battery_effective_kwh']:.2f} kWh","Used in simulation"],
            ["Battery power addition",   f"{t['battery_power_addition_kw']:.2f} kW", ""],
            ["Total battery power",      f"{t['total_battery_power_kw']:.2f} kW",    ""],
            ["PV addition",              f"{t['pv_addition_kw']:.2f} kW",        "Added to cover day gap"],
            ["Total PV in system",       f"{t['total_pv_kw']:.2f} kW",           "Existing + addition"],
            ["Required inverter",        f"{t['required_inverter_kw']:.1f} kW",
             "Peak import power + total PV"],
            ["Inverter addition needed", f"{t['inverter_addition_kw']:.1f} kW",  ""],
            ["Roof constrained",         "YES" if t["roof_constrained"] else "No",""],
            ["Optimisation iterations",  str(t["optimisation_iterations"]),       ""],
            ["Reliability achieved",     f"{t['reliability_pct']:.1f}%",         ""],
            ["Residual import / day",    f"{t['residual_import_kwh']:.4f} kWh",  ""],
            ["  Night residual",         f"{t['residual_night_kwh']:.4f} kWh",   ""],
            ["  Day residual",           f"{t['residual_day_kwh']:.4f} kWh",     ""],
        ]
        det_tbl = Table(detail_rows, colWidths=[CW*0.40, CW*0.22, CW*0.38])
        hdr_bg  = TIER_COLOURS[pct] if pct < 75 else C_MID
        det_tbl.setStyle(_table_style(header_bg=hdr_bg))
        story.append(det_tbl)

        # SOH projection for new battery
        story.append(Spacer(1, 0.25 * cm))
        story.append(Paragraph(
            f"New Battery SOH Degradation Projection "
            f"({t['nominal_addition_kwh']:.2f} kWh effective ordered as "
            f"{t['nameplate_to_order_kwh']:.2f} kWh nameplate)",
            St["body_bold"],
        ))
        story.append(Spacer(1, 0.1 * cm))

        soh_hdr = [Paragraph(x, St["table_hdr"]) for x in
                   ["Year", "SOH %", "Usable kWh", "Covers night need?"]]
        soh_rows = [soh_hdr]
        for row in t["soh_projection"]:
            covers  = ("✓  Yes" if row["usable_kwh"] >= t["usable_energy_needed_kwh"]
                       else "✗  No")
            eol_tag = "  ← EOL" if row["year"] == inp["new_battery_design_years"] else ""
            soh_rows.append([
                f"{row['year']}{eol_tag}",
                f"{row['soh_pct']:.1f}%",
                f"{row['usable_kwh']:.2f}",
                covers,
            ])

        soh_tbl = Table(soh_rows, colWidths=[CW*0.18, CW*0.18, CW*0.22, CW*0.42])
        sts = _table_style()
        for ri, row in enumerate(t["soh_projection"], start=1):
            if row["usable_kwh"] < t["usable_energy_needed_kwh"]:
                sts.add("BACKGROUND", (0, ri), (-1, ri), colors.HexColor("#fde8e8"))
                sts.add("TEXTCOLOR",  (3, ri), (3, ri),  C_RED)
        soh_tbl.setStyle(sts)
        story.append(soh_tbl)

        for w in t["warnings"]:
            story.append(Spacer(1, 0.1 * cm))
            story.append(Paragraph(f"⚠  {w}", St["warn"]))

        if pct < 100:
            story.append(Spacer(1, 0.4 * cm))
            story.append(HRFlowable(width="100%", thickness=0.5,
                                    color=colors.HexColor("#cccccc")))

    # ── SECTION 5: METHODOLOGY ────────────────────────────────────────────
    story.append(PageBreak())
    story.append(_section_header("5. Methodology Notes", St))
    story.append(Spacer(1, 0.2 * cm))

    notes = [
        ("<b>Gap-based sizing (key fix v5):</b>  For each tier the engine first "
         "simulates 24 hours using ONLY the existing equipment against the tier's "
         "import target. Whatever the existing system cannot cover becomes the "
         "<i>gap</i>. Day gaps drive PV additions; night gaps drive battery additions. "
         "This prevents the common error of adding equipment to handle import that "
         "the existing kit already covers."),
        ("<b>Day import → PV:</b>  PV addition is sized so its net output equals "
         "the day gap plus battery charging losses, divided by peak sun hours and "
         "adjusted for system losses."),
        ("<b>Night import → Battery:</b>  Battery addition is sized so its usable "
         "energy equals the night gap × safety margin. This is divided by DoD "
         "for gross capacity, then by EOL SOH to get the nameplate to order today."),
        ("<b>SOH derate on existing battery:</b>  Only the SOH-derated effective "
         "capacity is credited to the existing system."),
        ("<b>Tier definitions:</b>  25/50/75% tiers apply fractions to the average "
         "day and average night import separately. The 100% tier uses the peak day "
         "and peak night figures — the system must handle the worst day ever."),
        ("<b>Inverter sizing:</b>  Required inverter = peak import power + total PV "
         "in system (worst-case simultaneous back-feed scenario)."),
        ("<b>Optimisation loop:</b>  After analytical sizing, a 24-hour simulation "
         "checks for residual import and adds 0.5 kW PV (day residual) or 1.0 kWh "
         "battery (night residual) until the target is met or the roof limit is hit."),
    ]
    for n in notes:
        story.append(Paragraph(f"• {n}", St["body"]))
        story.append(Spacer(1, 0.18 * cm))

    doc.build(story)
    print(f"\nPDF report saved → {pdf_path}")


# =============================================================================
# SECTION 8 — CSV TEMPLATE WRITER
# =============================================================================

def write_sample_csv(path: str = "input_v5.csv") -> None:
    rows = [
        ("parameter",                "value"),
        ("# --- Six direct import inputs ---", ""),
        ("avg_daily_import_kwh",     "18.0"),
        ("avg_day_import_kwh",       "10.5"),
        ("avg_night_import_kwh",     "7.5"),
        ("peak_daily_import_kwh",    "34.0"),
        ("peak_day_import_kwh",      "20.0"),
        ("peak_night_import_kwh",    "14.0"),
        ("peak_import_power_kw",     "9.5"),
        ("avg_load_power_kw",        "3.2"),
        ("# --- Existing equipment ---", ""),
        ("current_pv_kw",            "3.0"),
        ("current_battery_kwh",      "10.0"),
        ("current_battery_power_kw", "5.0"),
        ("current_inverter_kw",      "5.0"),
        ("# --- Battery SOH ---",    ""),
        ("current_battery_soh",      "0.85"),
        ("new_battery_eol_soh",      "0.80"),
        ("new_battery_design_years", "10"),
        ("# --- Battery technical ---", ""),
        ("battery_dod",              "0.85"),
        ("battery_efficiency",       "0.90"),
        ("battery_safety_margin",    "1.20"),
        ("# --- Solar resource ---", ""),
        ("sun_hours_per_day",        "5.5"),
        ("system_loss_factor",       "0.15"),
        ("roof_available_kw",        "999"),
        ("# --- Inverter rule ---",  ""),
        ("inverter_sizing_rule",     "auto"),
        ("required_inverter_kw",     "0"),
        ("# --- Algorithm ---",      ""),
        ("max_iterations",           "50"),
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for r in rows:
            writer.writerow(r)
    print(f"Sample CSV written → {path}")


# =============================================================================
# SECTION 9 — CLI
# =============================================================================

def parse_args():
    p = argparse.ArgumentParser(
        description="Import-driven solar sizing v5: gap-based PV and battery sizing"
    )
    p.add_argument("--csv",          default=None,
                   help="Input CSV path (omit to write sample CSV and run defaults)")
    p.add_argument("--output-json",  default=None)
    p.add_argument("--pdf",          default="grid_independence_report_v5.pdf")
    p.add_argument("--write-sample-csv", action="store_true",
                   help="Write input_v5.csv sample and exit")
    return p.parse_args()


def main():
    args = parse_args()

    if args.write_sample_csv:
        write_sample_csv()
        return

    if args.csv:
        print(f"\nLoading: {args.csv}")
        cd = load_csv(args.csv)
    else:
        print("\nNo CSV provided — running with hard-coded defaults.")
        cd = load_csv.__wrapped__({}) if hasattr(load_csv, "__wrapped__") else {}
        # Fall back: build cd from defaults by calling load_csv on a temp file
        import tempfile, os
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".csv",
                                          delete=False, encoding="utf-8")
        write_sample_csv(tmp.name)
        tmp.close()
        cd = load_csv(tmp.name)
        os.unlink(tmp.name)

    result = import_driven_sizing(cd)
    print_result(result)
    generate_pdf(result, args.pdf)

    if args.output_json:
        with open(args.output_json, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, default=str)
        print(f"JSON saved → {args.output_json}")

    print(f"\n{'='*72}\n")


if __name__ == "__main__":
    main()
