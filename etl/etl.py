import os
import json
import zipfile
import psycopg2
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
    for item in json_data:
        try:
            # Verificar se a linha é válida
            line = item.get("linha")
            if line not in VALID_LINES:
                continue

            # Corrigir vírgula para ponto
            lat = float(item["latitude"].replace(",", "."))
            lon = float(item["longitude"].replace(",", "."))

            cursor.execute("""
                INSERT INTO gps_data (ordem, linha, datahora, datahoraenvio, datahoraservidor, velocidade, latitude, longitude, geom)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
            """, (
                item["ordem"],
                line,
                int(item["datahora"]),
                int(item["datahoraenvio"]),
                int(item["datahoraservidor"]),
                int(item["velocidade"]),
                lat,
                lon,
                lon, lat
            ))

        except Exception as e:
            continue  # ou logar erro

def run_etl():
    conn = connect()
    cur = conn.cursor()

    gps_dir = "/app/dados/gps"

    for zip_name in tqdm(os.listdir(gps_dir)):
        if not zip_name.endswith(".zip"):
            continue

        with zipfile.ZipFile(os.path.join(gps_dir, zip_name), 'r') as archive:
            for filename in archive.namelist():
                if not filename.endswith(".json"):
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