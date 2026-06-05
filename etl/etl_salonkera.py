"""
ETL SalonKera — Boutique Toamasina
Gère les incohérences du système SalonKera :
  - Séparateur CSV : point-virgule (;)
  - Dates au format DD/MM/YYYY
  - Sexe : "Homme"/"Femme" au lieu de M/F
  - Montants avec espace millier : "18 000" → 18000
  - Booléens : 1/0 au lieu de Oui/Non
  - Valeurs manquantes dans certaines colonnes
"""

import os
import re
import pandas as pd
from psycopg2.extras import execute_values
from datetime import datetime
from dotenv import load_dotenv
from db_config import get_connection

load_dotenv(os.path.join(os.path.dirname(__file__), "../.env"))

DATA_DIR = os.path.join(os.path.dirname(__file__), "../data/salonkera")

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

# Colonnes contenant des dates au format DD/MM/YYYY dans SalonKera
DATE_COLUMNS = {
    "clients":            ["date_inscription"],
    "ventes":             ["date_achat"],
    "actions_crm":        ["date"],
    "rendez_vous":        ["date_rdv"],
    "avis_clients":       ["date_avis"],
    "programme_fidelite": ["date_adhesion"],
    "promotions":         ["date_debut", "date_fin"],
    "remboursements":     ["date_demande", "date_traitement"],
    "abonnements":        ["date_debut", "date_fin", "prochaine_facturation"],
}

# Colonnes contenant des montants avec espace millier "18 000"
AMOUNT_COLUMNS = {
    "clients":            ["budget_mensuel"],
    "ventes":             ["prix_total"],
    "actions_crm":        ["cout"],
    "rendez_vous":        ["prix"],
    "promotions":         ["chiffre_affaires_genere"],
    "remboursements":     ["montant_rembourse"],
    "abonnements":        ["prix_mensuel"],
    "stocks":             ["valeur_stock"],
    "employes":           ["salaire"],
}


def _fix_date(val: str) -> str:
    """Convertit DD/MM/YYYY → YYYY-MM-DD. Laisse intact si autre format."""
    if pd.isna(val) or not isinstance(val, str):
        return val
    val = val.strip()
    if re.match(r"^\d{2}/\d{2}/\d{4}$", val):
        d, m, y = val.split("/")
        return f"{y}-{m}-{d}"
    return val


def _fix_amount(val) -> str:
    """Supprime les espaces dans les montants : '18 000' → '18000'."""
    if pd.isna(val):
        return val
    return str(val).replace(" ", "").replace(" ", "")


def _fix_sexe(val: str) -> str:
    """Normalise Homme/Femme → M/F."""
    if pd.isna(val):
        return val
    mapping = {"Homme": "M", "Femme": "F", "homme": "M", "femme": "F"}
    return mapping.get(str(val).strip(), val)


def _fix_boolean(val) -> str:
    """Normalise 1/0 → Oui/Non."""
    if pd.isna(val):
        return val
    mapping = {"1": "Oui", "0": "Non", 1: "Oui", 0: "Non"}
    return mapping.get(val, str(val))


def transform(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Applique toutes les corrections sur un DataFrame SalonKera."""
    corrections = []

    # Dates
    for col in DATE_COLUMNS.get(table_name, []):
        if col in df.columns:
            avant = df[col].copy()
            df[col] = df[col].apply(_fix_date)
            nb = (avant != df[col]).sum()
            if nb > 0:
                corrections.append(f"dates '{col}' : {nb} valeurs corrigées (DD/MM/YYYY → YYYY-MM-DD)")

    # Montants
    for col in AMOUNT_COLUMNS.get(table_name, []):
        if col in df.columns:
            avant = df[col].astype(str)
            df[col] = df[col].apply(_fix_amount)
            nb = (avant != df[col].astype(str)).sum()
            if nb > 0:
                corrections.append(f"montants '{col}' : {nb} valeurs corrigées (espace millier supprimé)")

    # Sexe
    if "sexe" in df.columns:
        avant = df["sexe"].copy()
        df["sexe"] = df["sexe"].apply(_fix_sexe)
        nb = (avant != df["sexe"]).sum()
        if nb > 0:
            corrections.append(f"sexe : {nb} valeurs normalisées (Homme/Femme → M/F)")

    # Booléens
    if "succes" in df.columns:
        avant = df["succes"].copy()
        df["succes"] = df["succes"].apply(_fix_boolean)
        nb = (avant != df["succes"]).sum()
        if nb > 0:
            corrections.append(f"succes : {nb} valeurs normalisées (1/0 → Oui/Non)")

    # Valeurs manquantes → chaîne vide
    nb_manquantes = df.isnull().sum().sum()
    if nb_manquantes > 0:
        df = df.fillna("")
        corrections.append(f"valeurs manquantes : {nb_manquantes} cellules remplacées par ''")

    if corrections:
        print(f"  [TRANSFORM] {table_name} :")
        for c in corrections:
            print(f"    → {c}")

    return df


def load_table(conn, table_name: str, source: str = "salonkera"):
    filepath = os.path.join(DATA_DIR, f"{table_name}.csv")
    if not os.path.exists(filepath):
        print(f"[SKIP] {filepath} introuvable")
        return 0

    # Détection automatique du séparateur (SalonKera utilise ;)
    with open(filepath, encoding="utf-8") as f:
        first_line = f.readline()
    sep = ";" if first_line.count(";") > first_line.count(",") else ","

    df = pd.read_csv(filepath, sep=sep, dtype=str)
    df = transform(df, table_name)
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
    print(f"[ETL SalonKera] Démarrage — {datetime.now()}")
    conn = get_connection()
    total = 0
    for table in TABLES:
        total += load_table(conn, table, source="salonkera")
    conn.close()
    print(f"[ETL SalonKera] Terminé — {total} lignes au total")
    return total


if __name__ == "__main__":
    run()
