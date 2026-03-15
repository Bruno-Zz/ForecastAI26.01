-- ============================================================
-- theBicycle account setup (run against the tenant DB for
-- theBicycle, e.g. forecastai_bicycle)
-- ============================================================
-- After running the normal schema.sql DDL on the new DB, insert
-- the parameter rows below to configure the ETL for Excel import.

-- 1. data_source parameter — tells ETL to use the Excel adapter
--    Adjust file_path to the actual workbook location on the server.
INSERT INTO zcube.parameters (parameter_type, name, parameters_set, is_default)
VALUES (
  'data_source',
  'Default',
  '{
    "source_type": "excel",
    "excel_source": {
      "file_path": "C:/allDev/ForecastAI2026.01/data/bicycle/bicycle_model.xlsx",
      "demand_sheet": "demand_actuals",
      "items_sheet":  "items",
      "sites_sheet":  "sites",
      "columns": {
        "unique_id": "unique_id",
        "item_id":   "item_id",
        "site_id":   "site_id",
        "channel":   "channel",
        "date":      "date",
        "qty":       "qty"
      }
    }
  }'::jsonb,
  TRUE
)
ON CONFLICT (parameter_type, name)
DO UPDATE SET parameters_set = EXCLUDED.parameters_set;

-- 2. etl parameter — weekly aggregation, 2-year lookback
INSERT INTO zcube.parameters (parameter_type, name, parameters_set, is_default)
VALUES (
  'etl',
  'Default',
  '{
    "query": {
      "min_observations": 8,
      "min_date": null,
      "max_date": null
    },
    "aggregation": {
      "frequency": "W",
      "method":    "sum"
    }
  }'::jsonb,
  TRUE
)
ON CONFLICT (parameter_type, name)
DO UPDATE SET parameters_set = EXCLUDED.parameters_set;

-- 3. Verify
SELECT parameter_type, name, is_default,
       parameters_set->>'source_type' AS source_type
FROM zcube.parameters
WHERE parameter_type IN ('data_source','etl')
ORDER BY 1, 2;
