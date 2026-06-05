import os
import requests
import pandas as pd
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
from db_config import get_connection

API_KEY = os.getenv("OPENWEATHER_API_KEY", "")
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

VILLES = [
    {"nom": "Antananarivo", "lat": -18.9137, "lon": 47.5361},
    {"nom": "Toamasina", "lat": -18.1443, "lon": 49.4023},
]

DATA_DIR = os.path.join(os.path.dirname(__file__), "../data/meteo")


def fetch_meteo_live(ville: dict) -> dict | None:
    if not API_KEY:
        print("[WARN] Pas de clé API — utilisation des données CSV de secours")
        return None

    params = {
        "lat": ville["lat"],
        "lon": ville["lon"],
        "appid": API_KEY,
        "units": "metric",
        "lang": "fr",
    }
    resp = requests.get(BASE_URL, params=params, timeout=10)
    if resp.status_code != 200:
        print(f"[WARN] API météo — HTTP {resp.status_code} pour {ville['nom']} → fallback CSV")
        return None
    data = resp.json()

    return {
        "date": datetime.utcnow().strftime("%Y-%m-%d"),
        "ville": ville["nom"],
        "temperature_max_c": data["main"]["temp_max"],
        "temperature_min_c": data["main"]["temp_min"],
        "humidite_pct": data["main"]["humidity"],
        "precipitation_mm": data.get("rain", {}).get("1h", 0.0),
        "conditions": data["weather"][0]["description"].capitalize(),
        "vent_kmh": round(data["wind"]["speed"] * 3.6, 1),
        "saison": _get_saison(datetime.utcnow().month),
        "uv_index": None,
    }


def load_csv_fallback(ville_nom: str) -> pd.DataFrame:
    filepath = os.path.join(DATA_DIR, f"meteo_{ville_nom.lower()}.csv")
    if os.path.exists(filepath):
        return pd.read_csv(filepath)
    return pd.DataFrame()


def _get_saison(month: int) -> str:
    if month in (11, 12, 1, 2, 3):
        return "Été (saison des pluies)"
    if month in (4, 5):
        return "Automne (transition)"
    if month in (6, 7, 8):
        return "Hiver (saison sèche)"
    return "Printemps (transition)"


def load_to_db(conn, df: pd.DataFrame):
    df["loaded_at"] = datetime.utcnow()
    cols = list(df.columns)
    rows = [tuple(row) for row in df.itertuples(index=False)]

    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS staging.raw_meteo CASCADE")
        col_defs = ", ".join(f'"{c}" TEXT' for c in cols)
        cur.execute(f"CREATE TABLE staging.raw_meteo ({col_defs})")
        execute_values(cur, "INSERT INTO staging.raw_meteo VALUES %s", rows)

    conn.commit()
    print(f"[OK] staging.raw_meteo — {len(df)} lignes chargées")


def run():
    print(f"[ETL Météo] Démarrage — {datetime.now()}")
    all_records = []

    for ville in VILLES:
        record = fetch_meteo_live(ville)
        if record:
            all_records.append(record)
        else:
            df_fallback = load_csv_fallback(ville["nom"])
            if not df_fallback.empty:
                all_records.extend(df_fallback.to_dict("records"))
                print(f"[FALLBACK] {ville['nom']} — {len(df_fallback)} lignes CSV")

    if not all_records:
        print("[ERROR] Aucune donnée météo disponible")
        return 0

    df = pd.DataFrame(all_records)
    conn = get_connection()
    load_to_db(conn, df)
    conn.close()
    print(f"[ETL Météo] Terminé — {len(df)} lignes")
    return len(df)


if __name__ == "__main__":
    run()
