import os
import json
import zipfile
import re
import psycopg2
from psycopg2.extras import execute_values
import time
from tqdm import tqdm

DB_PARAMS = {
    "host": "postgis",
    "dbname": "busdata",
    "user": "user",
    "password": "pass",
    "port": "5432"
}

VALID_LINES = {
    "483", "864", "639", "3", "309", "774", "629", "371", "397", "100", "838", "315", "624", "388", "918",
    "665", "328", "497", "878", "355", "138", "606", "457", "550", "803", "917", "638", "2336", "399", "298",
    "867", "553", "565", "422", "756", "292", "554", "634", "232", "415", "2803", "324", "852",
    "557", "759", "343", "779", "905", "108"
}

def connect(retries=10, delay=3):
    for attempt in range(retries):
        try:
            return psycopg2.connect(**DB_PARAMS)
        except psycopg2.OperationalError as e:
            print(f"Database not ready, retrying in {delay} seconds... ({attempt+1}/{retries})")
            time.sleep(delay)
    raise Exception("Could not connect to the database after several attempts.")

def process_file(json_data, cursor):
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
        execute_values(cursor, """
            INSERT INTO gps_data (
                ordem, linha, datahora, datahoraenvio, datahoraservidor, velocidade, longitude, latitude, geom
            )
            VALUES %s
            ON CONFLICT DO NOTHING
        """, rows, template="(%s, %s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))")

def run_etl():
    conn = connect()
    cur = conn.cursor()

    gps_dir = "/app/dados/gps"
    filename_pattern = re.compile(r"2024-\d{2}-\d{2}_(0[6-9]|1[0-9]|2[0-3])\.json$")

    for zip_name in tqdm(os.listdir(gps_dir)):
        if not zip_name.endswith(".zip"):
            continue

        with zipfile.ZipFile(os.path.join(gps_dir, zip_name), 'r') as archive:
            for filename in archive.namelist():
                base_filename = os.path.basename(filename)
                if not filename_pattern.match(base_filename):
                    print(f"Skipping file {filename} due to pattern mismatch")
                    continue

                with archive.open(filename) as f:
                    try:
                        data = json.load(f)
                        process_file(data, cur)
                        conn.commit()
                    except Exception as e:
                        print(f"Erro ao processar {filename}: {e}")
                        conn.rollback()

    cur.close()
    conn.close()

if __name__ == "__main__":
    run_etl()