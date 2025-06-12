-- Increases the memory settings for PostgreSQL to improve performance
ALTER SYSTEM SET shared_buffers = '1GB';
ALTER SYSTEM SET work_mem = '16MB';
ALTER SYSTEM SET maintenance_work_mem = '256MB';
SELECT pg_reload_conf();

CREATE EXTENSION IF NOT EXISTS postgis;

-- Function: make_square_grid (EXCLUDE)
-- Purpose: Generates a square grid within the spatial extent of a given table's geometry column.
-- Parameters:
--   p_table_name: Name of the table containing the geometry column.
--   p_grid_size: Size of each square in the grid (in degrees).
-- Returns:
--   A table with three columns:
--     - grid_i: Row index of the square in the grid.
--     - grid_j: Column index of the square in the grid.
--     - square_geom: Geometry of the square (as a polygon).
CREATE OR REPLACE FUNCTION public.make_square_grid(
	p_table_name text,
	p_grid_size double precision
)
RETURNS TABLE(
    grid_i integer, 
    grid_j integer, 
    square_geom geometry
) 
LANGUAGE 'plpgsql'
COST 100
STABLE PARALLEL SAFE 
ROWS 1000
AS $BODY$
DECLARE
	v_bounds geometry;  -- Bounding box of the table
BEGIN
	-- 1. Get spatial extent of the points from the table
	EXECUTE format(
		'SELECT ST_SetSRID(ST_EstimatedExtent(%L, ''geom'')::geometry, 4326)', p_table_name
    ) INTO v_bounds;

	IF v_bounds IS NULL THEN
        RAISE EXCEPTION
			'Table % does not contain any geometry (column "geom")',
          	p_table_name;
    END IF;

	-- 2. Make a square grid with these boundaries
	RETURN QUERY 
		SELECT g.i AS grid_i, g.j AS grid_j, g.geom AS square_geom 
		FROM ST_SquareGrid(p_grid_size, v_bounds) as g;
END
$BODY$;

-- Function: make_grid
-- Purpose: Generates a grid of squares based on the spatial extent of a given table's geometry column.
-- Parameters:
--   p_table_name: Name of the table containing the geometry column.
--   p_grid_size: Size of each square in the grid (in degrees).
-- Returns:
--   A table with five columns:
--     - pts_count: Number of points in the square.
--     - grid_geom: Geometry of the square (as a polygon).
--     - mean_point: Mean point of the square (centroid).
--     - grid_i: Row index of the square in the grid.
--     - grid_j: Column index of the square in the grid.
CREATE OR REPLACE FUNCTION public.make_grid(
	p_table_name text,
	p_grid_size double precision
)
RETURNS TABLE(
    pts_count bigint,
    grid_geom geometry,
    mean_point geometry,
    grid_i integer, 
    grid_j integer
) 
LANGUAGE 'plpgsql'
COST 100
STABLE PARALLEL SAFE 
ROWS 1000
AS $BODY$
DECLARE
	v_bounds geometry;  -- Bounding box of the table
BEGIN
	-- 1. Get spatial extent of the points from the table
	EXECUTE format(
		'SELECT ST_SetSRID(ST_EstimatedExtent(%L, ''geom'')::geometry, 4326)', p_table_name
    ) INTO v_bounds;

	IF v_bounds IS NULL THEN
        RAISE EXCEPTION
			'Table % does not contain any geometry (column "geom")',
          	p_table_name;
    END IF;

    -- 2. Make a square grid with these boundaries
    RETURN QUERY EXECUTE format('
        SELECT
            COUNT(pts.geom) AS pts_count,
            g.geom AS grid_geom,
            ST_Centroid(ST_Collect(pts.geom)) AS mean_point,
            g.i AS grid_i, 
            g.j AS grid_j
        FROM
            %I AS pts
        INNER JOIN
            ST_SquareGrid(%L, %L) AS g
        ON 
            ST_Intersects(pts.geom, g.geom)
        GROUP BY 
            g.i, g.j, g.geom
    ', p_table_name, p_grid_size, v_bounds);
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
--     - grid_geom: Geometry of the square (as a polygon).
--     - mean_point: Mean point of the square (centroid).
--     - grid_i: Row index of the square in the grid.
--     - grid_j: Column index of the square in the grid.
CREATE OR REPLACE FUNCTION public.make_grid(
	p_table_name text,
    p_bus_line text,
	p_grid_size double precision
)
RETURNS TABLE(
    pts_count bigint,
    grid_geom geometry,
    mean_point geometry,
    grid_i integer, 
    grid_j integer
) 
LANGUAGE 'plpgsql'
COST 100
STABLE PARALLEL SAFE 
ROWS 1000
AS $BODY$
DECLARE
	v_bounds geometry;  -- Bounding box of the table
BEGIN
	-- 1. Get spatial extent of the points from the table
	EXECUTE format(
		'SELECT ST_SetSRID(ST_EstimatedExtent(%L, ''geom'')::geometry, 4326)', p_table_name
    ) INTO v_bounds;

	IF v_bounds IS NULL THEN
        RAISE EXCEPTION
			'Table % does not contain any geometry (column "geom")',
          	p_table_name;
    END IF;

    -- 2. Make a square grid with these boundaries
    RETURN QUERY EXECUTE format('
        SELECT
            COUNT(pts.geom) AS pts_count,
            g.geom AS grid_geom,
            ST_Centroid(ST_Collect(pts.geom)) AS mean_point,
            g.i AS grid_i, 
            g.j AS grid_j
        FROM
            %I AS pts
        INNER JOIN
            ST_SquareGrid(%L, %L) AS g ON ST_Intersects(pts.geom, g.geom)
        WHERE
            linha = %L
        GROUP BY 
            g.i, g.j, g.geom
    ', p_table_name, p_grid_size, v_bounds, p_bus_line);
END
$BODY$;

-- Function: estimate_bus_trajectory
-- Purpose: Estimates the trajectory of a bus line based on its order and line number.
-- Parameters:
--   p_table_name: Name of the table containing GPS data.
--   p_bus_line: The bus line to analyze.
--   p_tolerance: Tolerance for simplifying the trajectory (default is 10).
-- Returns:
--   A table with the following columns:
--     - ordem: The bus id.
--     - trajectory: A simplified line geometry representing the bus trajectory.
CREATE OR REPLACE FUNCTION public.estimate_bus_trajectory(
	p_table_name text,
	p_bus_line text,
    tolerance double precision DEFAULT 10
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
        SELECT ordem, ST_Transform(geom, 3857) AS geom, to_timestamp(datahoraservidor/1000) AS ts
        FROM %I
        WHERE linha = %L',
        p_table_name, p_bus_line
    );

	-- 2. Return a simplified line geometry of the bus trajectory
    RETURN QUERY EXECUTE format('
        SELECT ordem, ST_Transform(ST_SimplifyPreserveTopology(ST_MakeLine(geom ORDER BY ts), %L), 4326) AS trajectory
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