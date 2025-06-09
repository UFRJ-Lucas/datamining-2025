CREATE EXTENSION IF NOT EXISTS postgis;

-- Function: make_square_grid
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

-- Function: estimate_bus_trajectory
-- Purpose: Estimates the trajectory of a bus line based on its order and line number.
-- Parameters:
--   p_table_name: Name of the table containing GPS data.
--   p_bus_line: The bus line to analyze.
--   p_tolerance: Tolerance for simplifying the trajectory (default is 0.01).
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
        SELECT ordem, ST_Simplify(ST_MakeLine(geom ORDER BY ts), %L) AS trajectory
        FROM temp_bus_points
        GROUP BY ordem',
        tolerance);
    
    -- Clean up temporary table
    DROP TABLE IF EXISTS temp_bus_points;
END
$BODY$;

-- Function: calculate_bus_dwell_statistics
-- Purpose: Calculates bus dwell statistics for a given bus line within a grid.
-- Parameters:
--   p_table_name: Name of the table containing GPS data.
--   p_bus_line: The bus line to analyze.
--   p_grid_size: Size of each square in the grid (in degrees).
-- Returns:
--   A table with the following columns:
--      - grid_i: Row index of the square in the grid.
--      - grid_j: Column index of the square in the grid.
--      - total_visits: Total number of bus visits in the grid cell.
--      - avg_dwell_minutes: Average time buses stay in the grid cell.
--      - median_dwell_minutes: Median time buses stay in the grid cell.
--      - max_dwell_minutes: Maximum dwell time in the grid cell.
--      - centroid: Centroid of the grid cell geometry.
CREATE OR REPLACE FUNCTION calculate_bus_dwell_statistics(
    p_table_name text,              		    -- GPS data table name
    p_bus_line text,                			-- Bus line to analyze
    p_grid_size double precision DEFAULT 0.001  -- Grid size in degrees
)
RETURNS TABLE(
    grid_i integer,                 -- Grid cell i coordinate
    grid_j integer,                 -- Grid cell j coordinate
    total_visits bigint,            -- Total number of bus visits
    avg_dwell_minutes numeric,      -- Average time buses stay
    median_dwell_minutes numeric,   -- Median time buses stay
    max_dwell_minutes numeric,      -- Maximum dwell time
    centroid geometry               -- Centroid of the grid cell
)
LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    RETURN QUERY EXECUTE format('
        WITH grid AS (
            SELECT * FROM public.make_square_grid(%L, %L)
        ),
        -- Get all bus points with timestamps
        points_with_time AS (
            SELECT 
                pts.ordem,
                g.grid_i,
                g.grid_j,
                g.square_geom,
                to_timestamp(pts.datahoraservidor/1000) AS time_point
            FROM 
                %I AS pts
            JOIN 
                grid AS g ON ST_Intersects(pts.geom, g.square_geom)
            WHERE 
                pts.linha = %L
            ORDER BY 
                pts.ordem, to_timestamp(pts.datahoraservidor/1000)
        ),
        -- Add lag values in a separate step (to avoid nested window functions)
        points_with_lag AS (
            SELECT
                ordem,
                grid_i,
                grid_j,
                square_geom,
                time_point,
                LAG(grid_i) OVER (PARTITION BY ordem ORDER BY time_point) AS prev_grid_i,
                LAG(grid_j) OVER (PARTITION BY ordem ORDER BY time_point) AS prev_grid_j,
                LAG(time_point) OVER (PARTITION BY ordem ORDER BY time_point) AS prev_time
            FROM 
                points_with_time
        ),
        -- Detect when a bus enters/exits a grid cell
        visit_markers AS (
            SELECT
                ordem,
                grid_i,
                grid_j,
                square_geom,
                time_point,
                CASE 
                    WHEN prev_grid_i IS DISTINCT FROM grid_i
                         OR prev_grid_j IS DISTINCT FROM grid_j
                         OR time_point - prev_time > INTERVAL ''3 minutes''
                    THEN 1 ELSE 0 
                END AS is_new_visit
            FROM 
                points_with_lag
        ),
        -- Create visit IDs by summing the markers
        visit_boundaries AS (
            SELECT
                ordem,
                grid_i,
                grid_j,
                square_geom,
                time_point,
                SUM(is_new_visit) OVER (PARTITION BY ordem ORDER BY time_point) AS visit_id
            FROM 
                visit_markers
        ),
        -- Calculate dwell time for each visit
        dwell_times AS (
            SELECT
                grid_i,
                grid_j,
                square_geom,
                ordem,
                visit_id,
                MIN(time_point) AS entry_time,
                MAX(time_point) AS exit_time,
                EXTRACT(EPOCH FROM (MAX(time_point) - MIN(time_point)))/60 AS dwell_minutes
            FROM
                visit_boundaries
            GROUP BY
                grid_i, grid_j, square_geom, ordem, visit_id
            HAVING
                COUNT(*) > 1 AND -- At least 2 points to calculate a valid dwell time
                EXTRACT(EPOCH FROM (MAX(time_point) - MIN(time_point)))/60 > 0.5 -- At least 30 seconds
        ),
        -- Aggregate statistics by grid cell
        grid_stats AS (
            SELECT
                grid_i,
                grid_j,
                square_geom,
                COUNT(*) AS total_visits,
                AVG(dwell_minutes)::numeric AS avg_dwell_minutes,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dwell_minutes)::numeric AS median_dwell_minutes,
                MAX(dwell_minutes)::numeric AS max_dwell_minutes
            FROM
                dwell_times
            GROUP BY
                grid_i, grid_j, square_geom
            HAVING
                COUNT(*) >= 3 -- Only include cells with multiple bus visits
        )
        -- Return the results
        SELECT
            grid_i,
            grid_j,
            total_visits,
            avg_dwell_minutes,
            median_dwell_minutes,
            max_dwell_minutes,
            ST_Centroid(square_geom) AS centroid
        FROM
            grid_stats
        ORDER BY
            median_dwell_minutes DESC
    ', 
    p_table_name, p_grid_size, p_table_name, p_bus_line);
END;
$BODY$;