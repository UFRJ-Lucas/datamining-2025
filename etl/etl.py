import os
import json
import zipfile
import re
import psycopg2
from psycopg2.extras import execute_values
from tqdm import tqdm

# Mudar os parâmetros de conexão com o banco de dados conforme necessário
DB_PARAMS = {
    "host": "localhost",
    "dbname": "busdata",
    "user": "user",
    "password": "pass",
    "port": "5432"
}

# Diretório onde os arquivos zip com dados GPS estão armazenados
DATA_DIR = "./dados/gps"

# Lista de linhas válidas
VALID_LINES = {
    "483", "864", "639", "3", "309", "774", "629", "371", "397", "100", "838", "315", "624", "388", "918",
    "665", "328", "497", "878", "355", "138", "606", "457", "550", "803", "917", "638", "2336", "399", "298",
    "867", "553", "565", "422", "756", "292", "554", "634", "232", "415", "2803", "324", "852",
    "557", "759", "343", "779", "905", "108"
}

def connect_to_database():
    print("Connecting to the database...")
    try:
        return psycopg2.connect(**DB_PARAMS)
    except Exception as e:
        raise Exception("Failed to connect to the database. Please check your connection parameters.")

def create_table_from_zip(conn, cursor, zip_name):
    zip_base_name = zip_name.split('.')[0]
    table_name = f"gps_{zip_base_name.replace('-', '_')}"

    """Create a new table for the zip file if it doesn't exist"""
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
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
        )
    """)
    conn.commit()

    print(f"Table {table_name} created or already exists.")
    return table_name

def create_indexes_for_table(conn, cursor, table_name):
    """Create indexes for the new table"""
    print(f"Creating indexes for table {table_name}...")
    cursor.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_{table_name}_geom ON {table_name} USING GIST (geom);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_datahora ON {table_name} (datahoraservidor);
    """)
    conn.commit()

def process_file(json_data, table_name, cursor):
    rows = []
    for item in json_data:
        try:
            # Verificar se a linha é válida
            line = item.get("linha")
            if line not in VALID_LINES:
                continue

            # Corrigir vírgula para ponto
            lat = float(item["latitude"].replace(",", "."))
            lon = float(item["longitude"].replace(",", "."))

            # Insere na lista de rows para inserção em lote
            rows.append((
                item["ordem"],
                line,
                item["datahora"],
                item["datahoraenvio"],
                item["datahoraservidor"],
                item["velocidade"],
                lon,
                lat,
                lon, lat
            ))
        except Exception as e:
            print(f"Erro ao processar item: {item} - {e}")

    if rows:
        execute_values(cursor, f"""
            INSERT INTO {table_name} (
                ordem, linha, datahora, datahoraenvio, datahoraservidor, velocidade, longitude, latitude, geom
            )
            VALUES %s
            ON CONFLICT DO NOTHING
        """, rows, template="(%s, %s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))")

def run_etl():
    conn = connect_to_database()
    cur = conn.cursor()
    gps_dir = DATA_DIR
    if not os.path.exists(gps_dir):
        print(f"Directory {gps_dir} does not exist.")
        return

    # Compila o padrão de nome de arquivo para filtrar os arquivos JSON
    # Os arquivos devem ter o formato "2024-MM-DD_HH.json" onde HH está entre 06 e 23
    filename_pattern = re.compile(r"2024-\d{2}-\d{2}_(0[6-9]|1[0-9]|2[0-3])\.json$")

    # Processa cada arquivo zip no diretório
    for zip_name in tqdm(os.listdir(gps_dir)):
        if not zip_name.endswith(".zip"):
            print(f"Skipping file '{zip_name}' as it is not a zip file.")
            continue

        table_name = create_table_from_zip(conn, cur, zip_name)

        with zipfile.ZipFile(os.path.join(gps_dir, zip_name), 'r') as archive:
            for filename in archive.namelist():
                base_filename = os.path.basename(filename)
                if not filename_pattern.match(base_filename):
                    print(f"Skipping file '{filename}' due to pattern mismatch.")
                    continue

                with archive.open(filename) as f:
                    try:
                        data = json.load(f)
                        process_file(data, table_name, cur)
                        conn.commit()
                    except Exception as e:
                        print(f"Error processing file '{filename}': {e}")
                        conn.rollback()

        create_indexes_for_table(conn, cur, table_name)

    # Fecha a conexão com o banco de dados
    cur.close()
    conn.close()
    print("ETL process completed successfully.")

if __name__ == "__main__":
    run_etl()