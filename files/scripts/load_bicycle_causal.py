"""
load_bicycle_causal.py
======================
Loads the causal (team/fleet/BOM) data from the bicycle Excel model into the
bicycle account's causal tables so that the Causal Forecasting screens are
populated.

Mapping:
  Excel assets        → causal_asset_type   (5 bike models)
  Excel bom           → causal_bom          (bike parts breakdown)
  Excel teams +
  Excel team_assets   → causal_fleet_plan   (# bikes per team per year)
  Excel team_demand   → causal_mdfh         (mean demand per bike per calendar day)
  Excel team_demand   → causal_task_cards   (scheduled maintenance tasks)
  (auto-created)      → causal_scenarios    (one "Base 2025" scenario)

Run from the files/ directory:
    python scripts/load_bicycle_causal.py
"""

import sys
import os
from pathlib import Path
from datetime import date

import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras

# ── Locate files/ directory ─────────────────────────────────────────────────
_files_dir = Path(__file__).resolve().parents[1]
if str(_files_dir) not in sys.path:
    sys.path.insert(0, str(_files_dir))

# ── Connect to thebicycle DB ────────────────────────────────────────────────
BICYCLE_DB = dict(
    host="localhost", port=5432, dbname="thebicycle",
    user="postgres", password="postgres"
)
SCHEMA = "zcube"
EXCEL_PATH = _files_dir.parent / "data" / "bicycle" / "bicycle_model.xlsx"


def get_conn():
    conn = psycopg2.connect(**BICYCLE_DB)
    conn.autocommit = False
    return conn


# ── Load Excel sheets ────────────────────────────────────────────────────────
print(f"Reading {EXCEL_PATH} …")
xl        = pd.ExcelFile(EXCEL_PATH)
assets_df = pd.read_excel(xl, "assets")
bom_df    = pd.read_excel(xl, "bom")
teams_df  = pd.read_excel(xl, "teams")
ta_df     = pd.read_excel(xl, "team_assets")   # team_assets
td_df     = pd.read_excel(xl, "team_demand")   # team_demand

# ── Valid item IDs in the DB ─────────────────────────────────────────────────
conn = get_conn()
cur  = conn.cursor()
cur.execute(f"SELECT id, name FROM {SCHEMA}.item")
valid_items = {r[0]: r[1] for r in cur.fetchall()}

# ── Team → site mapping (teams order from their country warehouse) ───────────
# Pro teams use Central Warehouse Brussels (100); amateurs use country WH.
COUNTRY_TO_SITE = {
    "AE": 100,   # UAE → Central Warehouse Brussels
    "NL": 205,   # Visma → Netherlands Country Warehouse
    "GB": 100,   # Ineos → Central Warehouse Brussels (no UK site)
    "FR": 201,   # TDF Amateurs → France Country Warehouse
    "DE": 202,   # Berlin CC → Germany Country Warehouse
    "ES": 203,   # Barcelona CC → Spain Country Warehouse
}
teams_df["site_id"] = teams_df["country"].map(COUNTRY_TO_SITE).fillna(100).astype(int)


# ════════════════════════════════════════════════════════════════════════════
# 1. causal_asset_type  (one row per bike model)
# ════════════════════════════════════════════════════════════════════════════
print("\n[1] Loading causal_asset_type …")
# Delete in FK-safe order
for tbl in ["causal_mdfh", "causal_bom", "causal_effectivity",
            "causal_fleet_plan", "causal_task_cards", "causal_asset_type"]:
    cur.execute(f"DELETE FROM {SCHEMA}.{tbl}")
conn.commit()

rows = []
for _, r in assets_df.iterrows():
    rows.append((
        int(r["asset_id"]),        # asset_type_id  (keep original ID)
        str(r["asset_code"]),      # code
        str(r["asset_name"]),      # name
        ["calendar_days"],         # removal_drivers (bicycle = calendar-day driver)
        50.0,                      # aog_cost_per_day (€50/day team bike unavailable)
        1.0,                       # mean_aog_days
    ))

psycopg2.extras.execute_values(
    cur,
    f"""INSERT INTO {SCHEMA}.causal_asset_type
            (asset_type_id, code, name, removal_drivers, aog_cost_per_day, mean_aog_days)
        VALUES %s
        ON CONFLICT (code) DO UPDATE
            SET name             = EXCLUDED.name,
                removal_drivers  = EXCLUDED.removal_drivers,
                aog_cost_per_day = EXCLUDED.aog_cost_per_day""",
    rows,
)
# Reset sequence so new inserts don't clash
cur.execute(f"SELECT setval(pg_get_serial_sequence('{SCHEMA}.causal_asset_type','asset_type_id'), "
            f"(SELECT MAX(asset_type_id) FROM {SCHEMA}.causal_asset_type) + 1, false)")
conn.commit()
print(f"  → {len(rows)} asset types upserted")


# ════════════════════════════════════════════════════════════════════════════
# 2. causal_bom  (bike → parts)
# ════════════════════════════════════════════════════════════════════════════
print("\n[2] Loading causal_bom …")
cur.execute(f"DELETE FROM {SCHEMA}.causal_bom")

bom_rows = []
seen = set()  # (asset_type_id, item_id)
for _, r in bom_df.iterrows():
    asset_type_id = int(r["asset_id"])
    item_id       = int(r["child_item_id"])
    if item_id not in valid_items:
        continue                             # skip sub-assembly IDs not in items table
    key = (asset_type_id, item_id)
    if key in seen:
        continue                             # UNIQUE constraint: take first occurrence
    seen.add(key)
    bom_rows.append((
        asset_type_id,
        item_id,
        float(r["quantity"]),               # qty_per_asset
        "calendar_days",                    # removal_driver
        None,                               # mdfh_override (filled by mdfh table)
        True,                               # is_lru
        1.0,                                # repair_yield
        None,                               # parent_bom_id
    ))

psycopg2.extras.execute_values(
    cur,
    f"""INSERT INTO {SCHEMA}.causal_bom
            (asset_type_id, item_id, qty_per_asset, removal_driver,
             mdfh_override, is_lru, repair_yield, parent_bom_id)
        VALUES %s
        ON CONFLICT (asset_type_id, item_id) DO UPDATE
            SET qty_per_asset = EXCLUDED.qty_per_asset,
                removal_driver = EXCLUDED.removal_driver""",
    bom_rows,
)
conn.commit()
print(f"  → {len(bom_rows)} BOM lines upserted "
      f"(covering {len({r[0] for r in bom_rows})} asset types, "
      f"{len({r[1] for r in bom_rows})} unique items)")


# ════════════════════════════════════════════════════════════════════════════
# 3. causal_scenarios  (one base scenario)
# ════════════════════════════════════════════════════════════════════════════
print("\n[3] Ensuring base causal scenario …")
cur.execute(f"SELECT scenario_id FROM {SCHEMA}.causal_scenarios WHERE is_base = TRUE LIMIT 1")
row = cur.fetchone()
if row:
    BASE_SCENARIO_ID = row[0]
    print(f"  → Base scenario already exists (id={BASE_SCENARIO_ID})")
else:
    cur.execute(
        f"""INSERT INTO {SCHEMA}.causal_scenarios
                (name, description, is_base, created_by, fleet_overrides, mdfh_overrides)
            VALUES (%s, %s, TRUE, 'system', '{{}}', '{{}}')
            RETURNING scenario_id""",
        ("Base Season 2025",
         "Baseline fleet utilisation — all 6 teams, 5 bike models, 2022-2025"),
    )
    BASE_SCENARIO_ID = cur.fetchone()[0]
    conn.commit()
    print(f"  → Created base scenario (id={BASE_SCENARIO_ID})")


# ════════════════════════════════════════════════════════════════════════════
# 4. causal_fleet_plan  (one annual row per team × bike model × year)
# ════════════════════════════════════════════════════════════════════════════
print("\n[4] Loading causal_fleet_plan …")
cur.execute(f"DELETE FROM {SCHEMA}.causal_fleet_plan WHERE scenario_id = 0")

fleet_rows = []
YEARS = [2022, 2023, 2024, 2025]

# Build team → site lookup
team_site = dict(zip(teams_df["team_id"], teams_df["site_id"]))
team_code  = dict(zip(teams_df["team_id"], teams_df["team_code"]))
asset_code_lookup = dict(zip(assets_df["asset_id"], assets_df["asset_code"]))

for _, ta in ta_df.iterrows():
    tid  = int(ta["team_id"])
    aid  = int(ta["asset_id"])
    qty  = int(ta["quantity"])
    site = team_site.get(tid, 100)
    tc   = team_code.get(tid, str(tid))
    ac   = asset_code_lookup.get(aid, str(aid))
    fleet_id = f"{tc}-{ac}"   # e.g. "UAE-CAN-AER"

    for yr in YEARS:
        days_in_year = 366 if yr % 4 == 0 else 365
        fleet_rows.append((
            0,                               # scenario_id = 0 (base)
            fleet_id,                        # asset_id  (team+model string)
            aid,                             # asset_type_id
            site,                            # site_id
            date(yr, 1, 1),                  # period_start
            date(yr, 12, 31),                # period_end
            0.0,                             # util_hours   (not applicable)
            0.0,                             # util_cycles  (not applicable)
            0.0,                             # util_landings(not applicable)
            float(qty * days_in_year),       # util_calendar_days = bikes × days
            True,                            # is_active
        ))

psycopg2.extras.execute_values(
    cur,
    f"""INSERT INTO {SCHEMA}.causal_fleet_plan
            (scenario_id, asset_id, asset_type_id, site_id,
             period_start, period_end,
             util_hours, util_cycles, util_landings, util_calendar_days, is_active)
        VALUES %s""",
    fleet_rows,
)
conn.commit()
print(f"  → {len(fleet_rows)} fleet plan rows inserted "
      f"({len(ta_df)} team×model combos × {len(YEARS)} years)")


# ════════════════════════════════════════════════════════════════════════════
# 5. causal_mdfh  (mean demand per bike per calendar day, per item × asset type)
#
#  Method: for each (asset_type_id, item_id) aggregate across ALL teams that
#  use that asset type.  Compute weighted average MDFH from team_demand.
#
#  For a single team+asset+item:
#    qty_per_event_per_bike = avg(event.qty) / fleet_qty_of_that_asset
#    interval_days          = total_span_days / (n_events - 1)  [if n>1]
#    mdfh_per_bike_per_day  = qty_per_event_per_bike / interval_days
#
#  Then weighted-average across teams by (total qty contributed).
# ════════════════════════════════════════════════════════════════════════════
print("\n[5] Computing and loading causal_mdfh …")
cur.execute(f"DELETE FROM {SCHEMA}.causal_mdfh")

td_df["date"] = pd.to_datetime(td_df["date"])

# Build fleet sizes: (team_id, asset_id) → quantity
fleet_size = {(int(r.team_id), int(r.asset_id)): int(r.quantity)
              for _, r in ta_df.iterrows()}

mdfh_accum = {}   # (asset_type_id, item_id) → list of (mdfh, weight, n_obs)

for (team_id, asset_id, item_id), grp in td_df.groupby(["team_id", "asset_id", "item_id"]):
    team_id   = int(team_id)
    asset_id  = int(asset_id)
    item_id   = int(item_id)
    if item_id not in valid_items:
        continue
    fleet_qty = fleet_size.get((team_id, asset_id), 1)

    grp = grp.sort_values("date")
    n_events = len(grp)
    avg_qty_per_event = grp["qty"].mean()
    qty_per_bike_per_event = avg_qty_per_event / fleet_qty

    if n_events > 1:
        span_days = (grp["date"].max() - grp["date"].min()).days
        interval_days = span_days / (n_events - 1)
    else:
        # Single event — use a reasonable default interval
        # Infer from the notes/event_type pattern (e.g. 28 days for chain pro)
        interval_days = 28.0

    if interval_days <= 0:
        continue

    mdfh_val = qty_per_bike_per_event / interval_days  # per bike per day
    weight   = grp["qty"].sum()                         # total qty consumed
    key      = (asset_id, item_id)
    mdfh_accum.setdefault(key, []).append((mdfh_val, weight, n_events))

mdfh_rows = []
for (asset_type_id, item_id), records in mdfh_accum.items():
    total_weight = sum(r[1] for r in records)
    if total_weight == 0:
        continue
    mdfh_mean = sum(r[0] * r[1] for r in records) / total_weight
    # Std dev across team-level estimates
    if len(records) > 1:
        mdfh_std = float(np.std([r[0] for r in records]))
    else:
        mdfh_std = mdfh_mean * 0.15   # assume 15% CV when only one team
    n_obs = sum(r[2] for r in records)
    mdfh_rows.append((
        item_id,
        asset_type_id,
        "calendar_days",
        float(round(float(mdfh_mean), 8)),
        float(round(float(mdfh_std), 8)),
        int(n_obs),
        "mle",
    ))

psycopg2.extras.execute_values(
    cur,
    f"""INSERT INTO {SCHEMA}.causal_mdfh
            (item_id, asset_type_id, removal_driver,
             mdfh_mean, mdfh_stddev, n_observations, fit_method)
        VALUES %s
        ON CONFLICT (item_id, asset_type_id, removal_driver) DO UPDATE
            SET mdfh_mean      = EXCLUDED.mdfh_mean,
                mdfh_stddev    = EXCLUDED.mdfh_stddev,
                n_observations = EXCLUDED.n_observations,
                fit_method     = EXCLUDED.fit_method,
                fitted_at      = NOW()""",
    mdfh_rows,
)
conn.commit()
print(f"  → {len(mdfh_rows)} MDFH rates computed and upserted")

# Print a few examples
print("  Sample MDFH rates (per bike per day):")
for row in mdfh_rows[:5]:
    item_name = valid_items.get(row[0], "?")
    asset_code = asset_code_lookup.get(row[1], "?")
    weekly_rate = row[3] * 7
    print(f"    {item_name[:40]:40s} × {asset_code}: "
          f"{row[3]:.5f}/day  (~{weekly_rate:.3f}/bike/week)")


# ════════════════════════════════════════════════════════════════════════════
# 6. causal_task_cards  (scheduled maintenance tasks from team_demand)
# ════════════════════════════════════════════════════════════════════════════
print("\n[6] Loading causal_task_cards …")
cur.execute(f"DELETE FROM {SCHEMA}.causal_task_cards")

# One card per (event_type, asset_type_id, item_id) with average qty per bike per event
tc_seen = {}
for _, r in td_df.iterrows():
    asset_id = int(r["asset_id"])
    item_id  = int(r["item_id"])
    team_id  = int(r["team_id"])
    if item_id not in valid_items:
        continue
    fleet_qty = fleet_size.get((team_id, asset_id), 1)
    qty_per_bike = float(r["qty"]) / fleet_qty
    key = (str(r["event_type"]), asset_id, item_id)
    tc_seen.setdefault(key, []).append(qty_per_bike)

tc_rows = []
for (event_type, asset_type_id, item_id), qtys in tc_seen.items():
    avg_qty = float(np.mean(qtys))
    tc_rows.append((event_type, asset_type_id, item_id, avg_qty, True))

psycopg2.extras.execute_values(
    cur,
    f"""INSERT INTO {SCHEMA}.causal_task_cards
            (check_type, asset_type_id, item_id, qty_per_event, is_mandatory)
        VALUES %s
        ON CONFLICT DO NOTHING""",
    tc_rows,
)
conn.commit()
print(f"  → {len(tc_rows)} task cards inserted")


# ════════════════════════════════════════════════════════════════════════════
# Summary
# ════════════════════════════════════════════════════════════════════════════
print("\n=== Causal data load complete ===")
for tbl in ["causal_asset_type", "causal_bom", "causal_fleet_plan",
            "causal_mdfh", "causal_task_cards", "causal_scenarios"]:
    cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{tbl}")
    print(f"  {tbl}: {cur.fetchone()[0]} rows")

conn.close()
print("\nDone. Restart the API and reload data to see the causal screens.")
