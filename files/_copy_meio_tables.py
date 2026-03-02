"""One-off script to copy MEIO tables from source Neon DB to local zcube schema."""
import json
import psycopg2
import psycopg2.extensions
from psycopg2.extras import execute_values, Json

# Register adapter so Python dicts are sent as JSONB
psycopg2.extensions.register_adapter(dict, Json)

# ── Source DB ──
src = psycopg2.connect(
    host='ep-gentle-rain-a5ba1hxu.us-east-2.aws.neon.tech',
    port=5432, database='scenario', user='ketteq',
    password='Zpg2TBlQydP1', sslmode='require',
    connect_timeout=30
)
src_cur = src.cursor()
print('Connected to source DB')

# ── Dest DB ──
dst = psycopg2.connect(host='localhost', port=5432, database='postgres', user='postgres', password='postgres')
dst_cur = dst.cursor()

# ═══════════════════════════════════════════════════════
# Drop existing tables in reverse dependency order
# ═══════════════════════════════════════════════════════
dst_cur.execute('DROP TABLE IF EXISTS zcube.on_hand CASCADE')
dst_cur.execute('DROP TABLE IF EXISTS zcube.on_hand_type CASCADE')
dst_cur.execute('DROP TABLE IF EXISTS zcube.item_chain CASCADE')
dst_cur.execute('DROP TABLE IF EXISTS zcube.bill_of_material CASCADE')
dst_cur.execute('DROP TABLE IF EXISTS zcube.route CASCADE')
dst_cur.execute('DROP TABLE IF EXISTS zcube.route_type CASCADE')
dst_cur.execute('DROP TABLE IF EXISTS zcube.meio_item CASCADE')
dst_cur.execute('DROP TABLE IF EXISTS zcube.item_site CASCADE')
dst.commit()

# ═══════════════════════════════════════════════════════
# 1. route_type (6 rows)
# ═══════════════════════════════════════════════════════
dst_cur.execute("""
    CREATE TABLE zcube.route_type (
        id                      BIGINT PRIMARY KEY,
        xuid                    TEXT NOT NULL,
        name                    TEXT NOT NULL,
        description             TEXT,
        planning_type           TEXT NOT NULL DEFAULT 'BUY',
        calc_supply_type_id     BIGINT,
        calc_dep_demand_type_id BIGINT
    )
""")
src_cur.execute("SELECT id, xuid, name, description, planning_type::text, calc_supply_type_id, calc_dep_demand_type_id FROM plan.route_type")
rows = src_cur.fetchall()
execute_values(dst_cur, "INSERT INTO zcube.route_type (id, xuid, name, description, planning_type, calc_supply_type_id, calc_dep_demand_type_id) VALUES %s", rows)
print(f'route_type: {len(rows)} rows')

# ═══════════════════════════════════════════════════════
# 2. meio_item (plan.item — 57268 rows)
# ═══════════════════════════════════════════════════════
dst_cur.execute("""
    CREATE TABLE zcube.meio_item (
        id          BIGINT PRIMARY KEY,
        xuid        TEXT NOT NULL,
        name        TEXT NOT NULL,
        description TEXT,
        attributes  JSONB,
        type_id     BIGINT,
        group_id    BIGINT
    )
""")
src_cur.execute("SELECT id, xuid, name, description, attributes, type_id, group_id FROM plan.item")
rows = src_cur.fetchall()
execute_values(dst_cur, "INSERT INTO zcube.meio_item (id, xuid, name, description, attributes, type_id, group_id) VALUES %s", rows, page_size=2000)
print(f'meio_item: {len(rows)} rows')

# ═══════════════════════════════════════════════════════
# 3. route (15221 rows)
# ═══════════════════════════════════════════════════════
dst_cur.execute("""
    CREATE TABLE zcube.route (
        id                          BIGINT PRIMARY KEY,
        item_id                     BIGINT NOT NULL,
        site_id                     BIGINT NOT NULL,
        supplier_id                 BIGINT,
        type_id                     BIGINT NOT NULL REFERENCES zcube.route_type(id),
        tag                         TEXT,
        source_item_id              BIGINT,
        source_site_id              BIGINT,
        bom_alternate               TEXT,
        min_qty                     DOUBLE PRECISION,
        mult_qty                    DOUBLE PRECISION,
        max_qty                     DOUBLE PRECISION,
        quota                       DOUBLE PRECISION,
        priority                    SMALLINT,
        lead_time                   SMALLINT,
        pick_pack_time              SMALLINT,
        transit_time                SMALLINT,
        inspection_time             SMALLINT,
        safety_lead_time            SMALLINT,
        ptf                         SMALLINT,
        lead_time_calendar_id       BIGINT,
        pick_pack_time_calendar_id  BIGINT,
        ship_calendar_id            BIGINT,
        transit_time_calendar_id    BIGINT,
        dock_calendar_id            BIGINT,
        inspection_time_calendar_id BIGINT,
        safety_lead_time_calendar_id BIGINT,
        ptf_calendar_id             BIGINT,
        yield                       DOUBLE PRECISION,
        unit_cost                   DOUBLE PRECISION,
        order_cost                  DOUBLE PRECISION,
        unit_cost_currency_id       BIGINT,
        order_cost_currency_id      BIGINT,
        uom_id                      BIGINT,
        end_date                    DATE
    )
""")
dst_cur.execute("CREATE INDEX IF NOT EXISTS idx_route_item_site ON zcube.route (item_id, site_id)")

src_cur.execute("SELECT * FROM plan.route")
cols = [d[0] for d in src_cur.description]
rows = src_cur.fetchall()
col_list = ",".join(cols)
execute_values(dst_cur, f"INSERT INTO zcube.route ({col_list}) VALUES %s", rows, page_size=2000)
print(f'route: {len(rows)} rows')

# ═══════════════════════════════════════════════════════
# 4. bill_of_material (17760 rows)
# ═══════════════════════════════════════════════════════
dst_cur.execute("""
    CREATE TABLE zcube.bill_of_material (
        id              BIGINT PRIMARY KEY,
        item_id         BIGINT NOT NULL,
        site_id         BIGINT NOT NULL,
        child_item_id   BIGINT NOT NULL,
        child_site_id   BIGINT NOT NULL,
        tag             TEXT,
        type_id         BIGINT,
        alternate       TEXT,
        item_qty        DOUBLE PRECISION NOT NULL DEFAULT 1,
        child_qty       DOUBLE PRECISION NOT NULL DEFAULT 1,
        attach_rate     DOUBLE PRECISION NOT NULL DEFAULT 1,
        start_date      DATE,
        end_date        DATE,
        "offset"        SMALLINT,
        child_uom_id    BIGINT,
        fixed_child_qty DOUBLE PRECISION,
        scrap           DOUBLE PRECISION
    )
""")
dst_cur.execute("CREATE INDEX IF NOT EXISTS idx_bom_item_site ON zcube.bill_of_material (item_id, site_id)")
dst_cur.execute("CREATE INDEX IF NOT EXISTS idx_bom_child ON zcube.bill_of_material (child_item_id, child_site_id)")

src_cur.execute("SELECT * FROM plan.bill_of_material")
cols = [d[0] for d in src_cur.description]
rows = src_cur.fetchall()
col_list_q = ",".join(f'"{c}"' for c in cols)
execute_values(dst_cur, f'INSERT INTO zcube.bill_of_material ({col_list_q}) VALUES %s', rows, page_size=2000)
print(f'bill_of_material: {len(rows)} rows')

# ═══════════════════════════════════════════════════════
# 5. item_chain (0 rows)
# ═══════════════════════════════════════════════════════
dst_cur.execute("""
    CREATE TABLE zcube.item_chain (
        id              BIGINT PRIMARY KEY,
        description     TEXT,
        item_id         BIGINT NOT NULL,
        site_id         BIGINT NOT NULL,
        child_item_id   BIGINT NOT NULL,
        child_site_id   BIGINT NOT NULL,
        policy_id       BIGINT
    )
""")
dst_cur.execute("CREATE INDEX IF NOT EXISTS idx_item_chain_item ON zcube.item_chain (item_id, site_id)")
print('item_chain: 0 rows (empty in source)')

# ═══════════════════════════════════════════════════════
# 6. on_hand_type (1 row)
# ═══════════════════════════════════════════════════════
dst_cur.execute("""
    CREATE TABLE zcube.on_hand_type (
        id                  BIGINT PRIMARY KEY,
        xuid                TEXT NOT NULL,
        name                TEXT NOT NULL,
        description         TEXT,
        planning_type       TEXT,
        allocation_sequence SMALLINT
    )
""")
src_cur.execute("SELECT id, xuid, name, description, planning_type::text, allocation_sequence FROM plan.on_hand_type")
rows = src_cur.fetchall()
execute_values(dst_cur, "INSERT INTO zcube.on_hand_type (id, xuid, name, description, planning_type, allocation_sequence) VALUES %s", rows)
print(f'on_hand_type: {len(rows)} rows')

# ═══════════════════════════════════════════════════════
# 7. on_hand (0 rows)
# ═══════════════════════════════════════════════════════
dst_cur.execute("""
    CREATE TABLE zcube.on_hand (
        id                      BIGINT PRIMARY KEY,
        item_id                 BIGINT NOT NULL,
        site_id                 BIGINT NOT NULL,
        type_id                 BIGINT REFERENCES zcube.on_hand_type(id),
        tag                     TEXT,
        qty                     DOUBLE PRECISION NOT NULL DEFAULT 0.0,
        unit_cost               DOUBLE PRECISION,
        unit_cost_currency_id   BIGINT,
        expiry_date             DATE,
        attributes              JSONB,
        feature_id              BIGINT
    )
""")
dst_cur.execute("CREATE INDEX IF NOT EXISTS idx_on_hand_item_site ON zcube.on_hand (item_id, site_id)")
print('on_hand: 0 rows (empty in source)')

# ═══════════════════════════════════════════════════════
# 8. item_site (new table — custom)
# ═══════════════════════════════════════════════════════
dst_cur.execute("""
    CREATE TABLE zcube.item_site (
        item_id       BIGINT NOT NULL,
        site_id       BIGINT NOT NULL,
        order_cost    DOUBLE PRECISION,
        holding_rate  DOUBLE PRECISION,
        cost_price    DOUBLE PRECISION,
        PRIMARY KEY (item_id, site_id)
    )
""")
dst_cur.execute("CREATE INDEX IF NOT EXISTS idx_item_site_item ON zcube.item_site (item_id)")
dst_cur.execute("CREATE INDEX IF NOT EXISTS idx_item_site_site ON zcube.item_site (site_id)")
print('item_site: created (empty)')

dst.commit()

# ═══════════════════════════════════════════════════════
# Verify all
# ═══════════════════════════════════════════════════════
print('\n=== Verification ===')
for tbl in ['route_type', 'meio_item', 'route', 'bill_of_material', 'item_chain', 'on_hand_type', 'on_hand', 'item_site']:
    dst_cur.execute(f'SELECT COUNT(*) FROM zcube.{tbl}')
    print(f'  zcube.{tbl:25s} {dst_cur.fetchone()[0]:>6} rows')

src_cur.close(); src.close()
dst_cur.close(); dst.close()
print('\nDone!')
