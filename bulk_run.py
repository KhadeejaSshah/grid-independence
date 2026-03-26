import subprocess
import os
import time

# Configuration
SYSTEMS_FILE = "oldsystemisb.md"
START_DATE = "2025-01-01"
END_DATE = "2026-03-23"
LOG_FILE = "bulk_run.log"

def log(message):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    formatted_msg = f"[{timestamp}] {message}"
    print(formatted_msg)
    with open(LOG_FILE, "a") as f:
        f.write(formatted_msg + "\n")

def process_systems():
    if not os.path.exists(SYSTEMS_FILE):
        log(f"Error: {SYSTEMS_FILE} not found.")
        return

    with open(SYSTEMS_FILE, "r") as f:
        systems = [line.strip() for line in f if line.strip()]

    log(f"Found {len(systems)} systems to process.")

    for i, system_id in enumerate(systems):
        summary_file = f"{system_id}_summary.csv"
        
        if os.path.exists(summary_file):
            log(f"[{i+1}/{len(systems)}] Skipping {system_id} (Results already exist).")
            continue

        log(f"[{i+1}/{len(systems)}] Processing {system_id}...")
        
        cmd = [
            "python3", "oldcombineall.py",
            "--system-id", system_id,
            "--start", START_DATE,
            "--end", END_DATE
        ]

        try:
            # We use subprocess.run and capture output to log it if needed
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                log(f"Successfully processed {system_id}.")
            else:
                log(f"Failed to process {system_id}. Return code: {result.returncode}")
                log(f"Error output: {result.stderr[:500]}...") # Log first 500 chars of error
                
        except Exception as e:
            log(f"Exception while processing {system_id}: {str(e)}")

    log("Bulk processing completed.")

if __name__ == "__main__":
    process_systems()
