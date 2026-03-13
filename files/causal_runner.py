"""CLI entry point for the causal demand generation step.

Usage:
    python causal_runner.py                          # run all active scenarios
    python causal_runner.py --scenario-ids 1 2 3    # run specific scenarios
    python causal_runner.py --fit-mdfh               # fit MDFH from removals first
    python causal_runner.py --no-meio                # causal only, skip MEIO feed
"""
import argparse
import logging
import sys
from pathlib import Path

# Ensure the files/ directory is in the path
_files_dir = Path(__file__).resolve().parent
if str(_files_dir) not in sys.path:
    sys.path.insert(0, str(_files_dir))

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


def main():
    p = argparse.ArgumentParser(description="Causal demand generation runner")
    p.add_argument("--scenario-ids", nargs="*", type=int,
                   help="Specific scenario IDs to run (default: all active)")
    p.add_argument("--fit-mdfh", action="store_true",
                   help="Fit MDFH from demand_actuals before running scenarios")
    p.add_argument("--no-meio", action="store_true",
                   help="Skip feeding results into the MEIO optimizer")
    p.add_argument("--horizon", type=int, default=24,
                   help="Number of horizon periods for MEIO rate aggregation (default: 24)")
    args = p.parse_args()

    if args.fit_mdfh:
        from causal.mdfh_fitter import fit_from_demand_actuals
        n = fit_from_demand_actuals()
        print(f"MDFH fitting complete: {n} rows saved")

    from causal.scenario_runner import run_causal_scenarios
    from db.db import get_conn, get_schema
    import pandas as pd

    if args.scenario_ids:
        ids = args.scenario_ids
    else:
        conn = get_conn()
        try:
            df = pd.read_sql(
                f"SELECT scenario_id FROM {get_schema()}.causal_scenarios ORDER BY scenario_id",
                conn
            )
        finally:
            conn.close()
        ids = df["scenario_id"].tolist()

    if not ids:
        print("No causal scenarios found. Create scenarios via the UI or API first.")
        sys.exit(0)

    print(f"Running {len(ids)} causal scenario(s): {ids}")
    result = run_causal_scenarios(
        ids,
        horizon_periods=args.horizon,
        feed_meio=not args.no_meio
    )
    print(result)


if __name__ == "__main__":
    main()
