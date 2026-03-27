#!/usr/bin/env python3
"""
Batch runner: read start/end from conf.yaml and system ids from islamabad_old_systems.csv
and invoke oldcombineall.py for each system id one-by-one, writing outputs to a per-system subdirectory.

Usage:
  python3 run_batch_oldcombineall.py [--conf conf.yaml] [--csv islamabad_old_systems.csv] [--out-dir batch_out] [--python python3] [--script oldcombineall.py]

The CSV may contain a header column named "system_id"; otherwise the first column is used.
"""
import argparse
import csv
import os
import subprocess
import sys
import yaml


def load_conf_dates(conf_path):
    with open(conf_path, 'r') as f:
        cfg = yaml.safe_load(f) or {}
    defaults = cfg.get('defaults', {})
    start = defaults.get('start_date')
    end = defaults.get('end_date')
    return start, end


def load_system_ids(csv_path):
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    ids = []
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        if reader.fieldnames:
            if 'system_id' in [h.lower() for h in reader.fieldnames]:
                # find actual header name case-insensitively
                sid_col = next(h for h in reader.fieldnames if h.lower() == 'system_id')
                for r in reader:
                    val = r.get(sid_col)
                    if val:
                        ids.append(val.strip())
                return ids
            else:
                # fallback: use first column
                first_col = reader.fieldnames[0]
                for r in reader:
                    val = r.get(first_col)
                    if val:
                        ids.append(val.strip())
                return ids
        # if no header, parse manually
    # try fallback to simple line-by-line
    with open(csv_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            # skip header-like lines
            if any(x in line.lower() for x in ['system_id', 'systemid']):
                continue
            ids.append(line.split(',')[0].strip())
    return ids


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--conf', default='conf.yaml')
    parser.add_argument('--csv', default='allidsdata.csv')
    parser.add_argument('--out-dir', default='.')
    parser.add_argument('--python', default=sys.executable)
    parser.add_argument('--script', default='oldcombineall.py')
    parser.add_argument('--continue-on-error', action='store_true')
    args = parser.parse_args()

    start, end = load_conf_dates(args.conf)
    if not start or not end:
        print(f"Start/end not found in {args.conf}. Aborting.")
        sys.exit(2)

    try:
        system_ids = load_system_ids(args.csv)
    except Exception as e:
        print(f"Failed to load CSV {args.csv}: {e}")
        sys.exit(2)

    os.makedirs(args.out_dir, exist_ok=True)

    print(f"Running {args.script} for {len(system_ids)} systems from {start} to {end}")

    for i, sid in enumerate(system_ids, start=1):
        sid = sid.strip()
        if not sid:
            continue
        print(f"\n[{i}/{len(system_ids)}] Processing system: {sid}")
        out_sub = os.path.join(args.out_dir, sid)
        os.makedirs(out_sub, exist_ok=True)
        cmd = [args.python, args.script, '--system-id', sid, '--start', str(start), '--end', str(end), '--out-dir', out_sub]
        try:
            # run and stream output
            proc = subprocess.run(cmd, check=False)
            rc = proc.returncode
            print(f"Finished {sid} (rc={rc})")
            if rc != 0 and not args.continue_on_error:
                print("Stopping due to non-zero return code. Use --continue-on-error to keep going.")
                sys.exit(rc)
        except Exception as e:
            print(f"Error running for {sid}: {e}")
            if not args.continue_on_error:
                raise

    print("\nBatch run complete.")


if __name__ == '__main__':
    main()
