import os
import pandas as pd
from psycopg2.extras import execute_values
from datetime import datetime
from db_config import get_connection

DATA_DIR = os.path.join(os.path.dirname(__file__), "../data/capilhair")

TABLES = [
    "clients",
    "produits",
    "ventes",
    "actions_crm",
    "rendez_vous",
    "avis_clients",
    "programme_fidelite",
    "stocks",
    "employes",
    "promotions",
    "remboursements",
    "abonnements",
]


def load_table(conn, table_name: str, source: str = "capilhair"):
    filepath = os.path.join(DATA_DIR, f"{table_name}.csv")
    if not os.path.exists(filepath):
        print(f"[SKIP] {filepath} introuvable")
        return 0

    df = pd.read_csv(filepath)
    df["source_boutique"] = source
    df["loaded_at"] = datetime.utcnow()

    dest_table = f"staging.raw_{source}_{table_name}"
    cols = list(df.columns)
    rows = [tuple(row) for row in df.itertuples(index=False)]

    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {dest_table} CASCADE")
        col_defs = ", ".join(f'"{c}" TEXT' for c in cols)
        cur.execute(f"CREATE TABLE {dest_table} ({col_defs})")
        execute_values(cur, f"INSERT INTO {dest_table} VALUES %s", rows)

    conn.commit()
    print(f"[OK] {dest_table} — {len(df)} lignes chargées")
    return len(df)


def run():
    print(f"[ETL CapilHair] Démarrage — {datetime.now()}")
    conn = get_connection()
    total = 0
    for table in TABLES:
        total += load_table(conn, table, source="capilhair")
    conn.close()
    print(f"[ETL CapilHair] Terminé — {total} lignes au total")
    return total


if __name__ == "__main__":
    run()
