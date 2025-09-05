-- Query Plan Analysis for Feature Engineering
-- This will show us exactly why PostgreSQL isn't using parallelism

EXPLAIN (ANALYZE, BUFFERS)
CREATE TABLE "03_primary_feature_table_test" AS
WITH
  -- Step 1: Base data with geospatial features
  base_features AS (
    SELECT
      t.*,
      -- Extract altitude directly from PostGIS geometry
      ST_Z(t.current_position::geometry) AS altitude,
      -- Simple geospatial classification
      CASE
        WHEN ST_Z(t.current_position::geometry) < 200 THEN 'Loading Zone A'
        WHEN ST_Z(t.current_position::geometry) > 250 THEN 'Dump Zone B' 
        ELSE 'Haul Road / Other'
      END AS location_type
    FROM "02_raw_telemetry_transformed" t
    WHERE current_position IS NOT NULL
    LIMIT 10000  -- Small sample for testing
  )
SELECT * FROM base_features;