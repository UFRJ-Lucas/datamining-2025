-- Increases the memory settings for PostgreSQL to improve performance
ALTER SYSTEM SET shared_buffers = '1GB';
ALTER SYSTEM SET work_mem = '16MB';
ALTER SYSTEM SET maintenance_work_mem = '256MB';
SELECT pg_reload_conf();

CREATE EXTENSION IF NOT EXISTS postgis;

-- Function: make_grid
-- Purpose: Generates a grid of squares based on the spatial extent of a given table's geometry column.
-- Parameters:
--   p_table_name: Name of the table containing the geometry column.
--   p_grid_size: Size of each square in the grid (in degrees).
-- Returns:
--   A table with five columns:
--     - pts_count: Number of points in the square.
--     - bus_line: The bus line associated with the points in the square.
--     - mean_point: Mean point of the square (centroid).
--     - grid_point: Point where the geom was snapped to.
CREATE OR REPLACE FUNCTION public.make_grid(
	p_table_name text,
	p_grid_size double precision
)
RETURNS TABLE(
    pts_count bigint,
    bus_line text,
    mean_point geometry,
    grid_point geometry
)
LANGUAGE 'plpgsql' 
AS $BODY$
BEGIN
    RETURN QUERY EXECUTE format('
        SELECT
            COUNT(*) AS pts_count,
            linha AS bus_line,
            ST_Centroid(ST_Collect(geom)) AS mean_point,
            ST_SnapToGrid(geom, %L) AS grid_point
        FROM %I
        GROUP BY bus_line, grid_point
    ', p_grid_size, p_table_name);
END
$BODY$;

-- Function: make_grid (with bus line filtering)
-- Purpose: Generates a grid of squares based on the spatial extent of a given table's geometry column, filtered by a specific bus line.
-- Parameters:
--   p_table_name: Name of the table containing the geometry column.
--   p_bus_line: The bus line to filter the points.
--   p_grid_size: Size of each square in the grid (in degrees).
-- Returns:
--   A table with five columns:
--     - pts_count: Number of points in the square.
--     - mean_point: Mean point of the square (centroid).
--     - grid_point: Point where the geom was snapped to.
CREATE OR REPLACE FUNCTION public.make_grid(
	p_table_name text,
    p_bus_line text,
	p_grid_size double precision
)
RETURNS TABLE(
    pts_count bigint,
    mean_point geometry,
    grid_point geometry
) 
LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    RETURN QUERY EXECUTE format('
        SELECT
            COUNT(*) AS pts_count,
            ST_Centroid(ST_Collect(geom)) AS mean_point,
            ST_SnapToGrid(geom, %L) AS grid_point
        FROM %I
        WHERE linha = %L
        GROUP BY grid_point
    ', p_grid_size, p_table_name, p_bus_line);
END
$BODY$;

-- Function: estimate_bus_trajectory
-- Purpose: Estimates the trajectory of a bus line based on its order and line number.
-- Parameters:
--   p_table_name: Name of the table containing GPS data.
--   p_bus_line: The bus line to analyze.
--   p_tolerance: Tolerance for simplifying the trajectory.
-- Returns:
--   A table with the following columns:
--     - ordem: The bus id.
--     - trajectory: A simplified line geometry representing the bus trajectory.
CREATE OR REPLACE FUNCTION public.estimate_bus_trajectory(
	p_table_name text,
	p_bus_line text,
    tolerance double precision DEFAULT 0.01
)
RETURNS TABLE(
    ordem text,
    trajectory geometry
)
LANGUAGE 'plpgsql'
COST 100
VOLATILE PARALLEL UNSAFE 
ROWS 1000
AS $BODY$
BEGIN
	-- 1. Get a temporary table of the bus points for the specified line
    EXECUTE format('
        CREATE TEMP TABLE IF NOT EXISTS temp_bus_points AS
        SELECT ordem, geom, to_timestamp(datahoraservidor/1000) AS ts
        FROM %I
        WHERE linha = %L',
        p_table_name, p_bus_line
    );

	-- 2. Return a simplified line geometry of the bus trajectory
    RETURN QUERY EXECUTE format('
        SELECT ordem, ST_SimplifyPreserveTopology(ST_MakeLine(geom ORDER BY ts), %L) AS trajectory
        FROM temp_bus_points
        GROUP BY ordem',
        tolerance);
    
    -- Clean up temporary table
    DROP TABLE IF EXISTS temp_bus_points;
END
$BODY$;

-- Function: get_trajectory_buffer
-- Purpose: Returns a buffer around the bus trajectory for a specified bus line.
-- Parameters:
--   p_start_point: The start point of the trajectory.
--   p_end_point: The end point of the trajectory.
--   p_trajectory_line: The trajectory line geometry.
--   p_buffer_size: The size of the buffer around the trajectory (in degrees).
-- Returns:
--   A table with a single column:
--     - trajectory_buffer: A geometry representing the buffer around the trajectory line.
CREATE OR REPLACE FUNCTION public.get_trajectory_buffer(
	p_start_point geometry,
    p_end_point geometry,
    p_trajectory_line geometry,
    p_buffer_size double precision DEFAULT 0.003
)
RETURNS TABLE(
    trajectory_buffer geometry
)
LANGUAGE 'plpgsql'
COST 100
VOLATILE PARALLEL UNSAFE 
ROWS 1000
AS $BODY$
BEGIN
	RETURN QUERY EXECUTE format('
        WITH fractions AS (
            SELECT 
                ST_LineLocatePoint(%L, %L) AS start_frac,
                ST_LineLocatePoint(%L, %L) AS end_frac
        )
        SELECT 
            ST_Buffer(
                ST_LineSubstring(%L, LEAST(start_frac, end_frac), GREATEST(start_frac, end_frac)), %L
            ) AS trajectory_buffer
        FROM fractions',
        p_trajectory_line, p_start_point, p_trajectory_line, p_end_point,
        p_trajectory_line, p_buffer_size
    );
END
$BODY$;

CREATE SEQUENCE IF NOT EXISTS gps_id_seq;

-- Table: gps_master
-- Purpose: Master table for all the other gps tables.
CREATE TABLE IF NOT EXISTS gps_data (
    id INTEGER DEFAULT nextval('gps_id_seq'),
    ordem TEXT,
    linha TEXT,
    datahora BIGINT,
    datahoraenvio BIGINT,
    datahoraservidor BIGINT,
    velocidade INTEGER,
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    geom GEOMETRY(Point, 4326)
) PARTITION BY RANGE (datahoraservidor);

CREATE INDEX IF NOT EXISTS idx_gps_data_geom ON gps_data USING GIST (geom);