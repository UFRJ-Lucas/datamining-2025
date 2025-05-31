CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE gps_data (
    id SERIAL PRIMARY KEY,
    ordem TEXT,
    linha TEXT,
    datahora BIGINT,
    datahoraenvio BIGINT,
    datahoraservidor BIGINT,
    velocidade INTEGER,
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    geom GEOMETRY(Point, 4326)
);

-- Índice para busca geográfica e por tempo
CREATE INDEX idx_gps_geom ON gps_data USING GIST (geom);
CREATE INDEX idx_gps_datahora ON gps_data (datahoraservidor);