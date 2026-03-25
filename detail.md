# Detailed Data Map

This document lists every important file in the repository, what data it reads/receives and where it comes from, and what the file produces.

Overview
- Purpose: gather system & energy data from Postgres, Scylla, an internal GraphQL API ("Giles"), and web bill pages; process into CSV summaries and a combined summary per system.
- Main orchestrators: `combine_all.py` and `oldcombineall.py`.
- Central config file: `conf.yaml` (DB/Scylla/Giles endpoints, defaults, secrets).

Important config keys (in `conf.yaml`)
- `postgresql`: host, port, user, password, dbname — used by Postgres code (`database.py`, `query_system.py`).
- `scylla`: host, port, username, password, keyspace, try_for_times — used by Scylla clients (`bms_soc.py`, `load_power_report.py`, `query_system.py`).
- `defaults`: `system_id`, `start_date`, `end_date` — used as CLI fallbacks by orchestrators.
- `secrets`: `LLAMA_PARSER_API_KEY`, `GEMINI_API_KEY` — used by `bill.py` for LlamaParse / Gemini.
- `giles.apiwork` / `giles.toseeunits`: `url`, `token` — used by `apiwork.py` and `toseeunits.py` (can be identical).

File-by-file (what each file reads and where the data comes from)

- `conf.yaml`
  - Reads: none at runtime (it's a config source for others).
  - Provides: DB credentials, Scylla creds, Giles API URL/token, Llama/Gemini keys, default system/date ranges.

- `database.py`
  - Reads: `conf.yaml.postgresql`.
  - Connects to: PostgreSQL database.
  - Exposes: DB connection helper used by `query_system.py`.

- `query_system.py`
  - Reads: `database.get_db_connection()` which uses `conf.yaml.postgresql`.
  - Queries: Postgres tables (systems, site details, console objects, tariffs, feeders, etc.) to return system metadata and site details.
  - Also reads: `conf.yaml.scylla` when performing Scylla queries.
  - Produces: dict with Postgres system fields and a list of recent Scylla energy rows (`query_scylla`).

- `apiwork.py`
  - Reads: `conf.yaml.giles.apiwork.url` and `conf.yaml.giles.apiwork.token` (or `GILES_API_URL` / `GILES_API_TOKEN` env vars).
  - Calls: internal GraphQL API (Giles) to fetch `systemV1` and `system` objects by `system_id`.
  - Produces: GraphQL JSON response with `siteDetails`, meter refs, city, etc.

- `toseeunits.py`
  - Reads: `conf.yaml.giles.toseeunits` (`url` and `token`) or env fallbacks.
  - Calls: Giles GraphQL queries `dailyWeather`, `systemDailyEnergyStats`, `systemHourlyEnergyStats`.
  - Optionally uses: `astral` library to compute sunrise/sunset when missing.
  - Produces:
    - `<system_id>_dashboard-data-daily.csv` (date, pvProduced, pvExported, gridConsumed, sunrise, sunset)
    - `<system_id>_dashboard-data-monthly.csv` (monthly aggregates, unitsT)
    - `<system_id>_dashboard-data-hourly.csv` (if hourly data fetched)
    - `import-export.csv` (single-row import/export summary)

- `energy_load.py`
  - Reads: (previously had hard-coded API URL/TOKEN; consider using `conf.yaml`) — currently uses `API_URL` and `TOKEN` in-file or update to read from `conf.yaml`.
  - Calls: Giles GraphQL `hourlyEnergy` to fetch hourly energy rows.
  - Uses: `astral` for sunrise/sunset calculation.
  - Produces:
    - `<system_id>_raw_energy_load.csv` (datetime, load, sunrise, sunset)
    - `<system_id>_daily_energy_summary.csv` (daily totals, night fraction)

- `compilerawdata.py`
  - Reads: raw hourly CSV (e.g., output of `energy_load.py`).
  - Produces:
    - `<system_id>_monthly_total_load.csv` (monthly total load)
    - `<system_id>_sun_hours_daily.csv` (sun hours per day)
    - Returns `avg_sun_hours` value.

- `bms_soc.py`
  - Reads: Scylla config from `conf.yaml.scylla`.
  - Queries: Scylla table `bms_soc_1d` for SOC daily aggregates.
  - Produces: `<system_id>_bms_soc_summary.csv` (lowest daily/monthly/yearly avg SOC and date range).

- `load_power_report.py`
  - Reads: Scylla config from `conf.yaml.scylla`.
  - Queries: Scylla table `load_combined_1d` for daily load stats.
  - Produces: `<system_id>_load_power_summary.csv` (peaks, seasonal peaks, growth factor, JSON of monthly/yearly peaks).

- `bill.py`
  - Reads: `conf.yaml.secrets` for LlamaParse / Gemini keys (fallback to env vars allowed).
  - Calls: PITC bill site pages defined in `CITY_URLS` (Islamabad/Lahore/Karachi) via Playwright to submit reference id and render bill page/PDF.
  - Calls: LlamaParse API to extract text from the generated PDF and Google Gemini to prettify/structure the extracted text.
  - Produces: saved PDF (temporary), raw parsed text, and `final_report_bill.txt` (beautified bill extraction).
  - Requirements: Playwright installed + browser binaries, network access to LlamaParse/Gemini APIs.

- `combine_all.py` and `oldcombineall.py`
  - Orchestrators reading from: `apiwork` (Giles), `query_system` (Postgres & Scylla), `bms_soc`, `load_power_report`, `energy_load`, `compilerawdata`, `toseeunits`, and `bill`.
  - Inputs: `--system-id`, `--start`, `--end` or `conf.yaml.defaults`.
  - Produces:
    - `<system_id>.csv` — combined single-row CSV containing JSON dumps (Postgres & API) and file paths to all produced CSVs, plus embedded bill text when available.
    - `<system_id>_summary.csv` — condensed metrics (import/night peaks, current PV/battery/SOC, sun hours, kWh estimate).

- `main.py`
  - Purpose: service entrypoint (see `README.md` for the simple API description). Reads DB config and exposes an endpoint when run.

How data typically flows (combine_all orchestration)
1. `apiwork.get_system_details(system_id)` → Giles GraphQL returns `siteDetails` (city, referenceNumber, meter info).
2. `query_system.query_system(system_id)` → Postgres returns enriched system metadata (console_object, battery fields, location, site details).
3. Scylla queries via `query_system.query_scylla`, `bms_soc.query_bms_soc`, `load_power_report.query_load_combined` for historical daily stats.
4. `energy_load` and/or `toseeunits` call Giles GraphQL hourly/daily endpoints to produce raw hourly CSVs, daily summaries, and import/export aggregates.
5. `compilerawdata` consumes raw hourly CSV to produce monthly totals and sun-hours aggregation.
6. `bill` optionally scrapes PITC bill site, renders PDF, extracts text via LlamaParse, and formats with Gemini.
7. `combine_all.py` collects all outputs and writes final combined CSV + summary CSV.

Environment and security notes
- Many files fall back to environment variables if `conf.yaml` entries are missing (common env names used: `GILES_API_URL`, `GILES_API_TOKEN`).
- Several files currently contain hard-coded tokens/keys in the repo. For production, move secrets to environment variables or a secrets manager and remove them from `conf.yaml`.
- Optional dependencies: `cassandra-driver` for Scylla access, `astral` for sunrise/sunset, `playwright` and browser binaries for `bill.py`, `llama_parse` & `google-generativeai` for parsing and beautification.

Next steps you might want
- Replace secrets in `conf.yaml` with environment variables and update all files to prefer env first.
- Add a central `config.py` helper to standardize loading and avoid duplicate code.
- Add a short runbook showing exact CLI invocations to reproduce combined output for a given `system_id`.

If you want, I can write this into the repo as `detail.md` (done) and/or update `README.md` to link to it or add a runbook snippet.
