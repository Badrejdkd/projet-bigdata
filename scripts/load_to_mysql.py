import boto3
import pandas as pd
import pyarrow.parquet as pq
import io
import mysql.connector

# ─── CONNEXIONS
s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin123",
)
BUCKET = "datalake"

conn = mysql.connector.connect(
    host="mysql",
    port=3306,
    database="books_dw",
    user="bigdata",
    password="bigdata123"
)
cursor = conn.cursor()

# ─── LIRE PARQUET GOLD DEPUIS MINIO ───────────────────────────────────────────
def read_gold(table_name):
    key = f"gold/analytics/{table_name}/data.parquet"
    try:
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        df = pq.read_table(io.BytesIO(body)).to_pandas()
        print(f"[OK] {table_name} : {len(df)} lignes lues")
        return df
    except Exception as e:
        print(f"[ERREUR] {table_name} : {e}")
        return pd.DataFrame()

# ─── HELPER INSERT ────────────────────────────────────────────────────────────
def safe(val, default=None):
    if val is None:
        return default
    try:
        if str(val) in ("nan", "NaT", "None", ""):
            return default
        return val
    except:
        return default

def safe_float(val):
    try:
        f = float(val)
        return None if str(f) == "nan" else round(f, 2)
    except:
        return None

def safe_int(val):
    try:
        return int(float(val))
    except:
        return 0

# ─── TABLE 1 : kpi_par_categorie ─────────────────────────────────────────────
def load_kpi_categorie():
    df = read_gold("kpi_par_categorie")
    if df.empty:
        return
    cursor.execute("TRUNCATE TABLE kpi_par_categorie")
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO kpi_par_categorie
                (categorie, nb_livres, prix_moyen, prix_min, prix_max,
                 note_moyenne, total_avis, source, date_gold)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            safe(row.get("categorie"), "Unknown"),
            safe_int(row.get("nb_livres")),
            safe_float(row.get("prix_moyen")),
            safe_float(row.get("prix_min")),
            safe_float(row.get("prix_max")),
            safe_float(row.get("note_moyenne")),
            safe_int(row.get("total_avis")),
            safe(row.get("source"), ""),
            safe(row.get("date_gold"), ""),
        ))
    conn.commit()
    print(f"[OK] kpi_par_categorie chargee : {len(df)} lignes")

# ─── TABLE 2 : distribution_notes ────────────────────────────────────────────
def load_distribution_notes():
    df = read_gold("distribution_notes")
    if df.empty:
        return
    cursor.execute("TRUNCATE TABLE distribution_notes")
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO distribution_notes
                (note_etoiles, categorie, nb_livres, prix_moyen, source, date_gold)
            VALUES (%s,%s,%s,%s,%s,%s)
        """, (
            safe_float(row.get("note_etoiles")),
            safe(row.get("categorie"), "Unknown"),
            safe_int(row.get("nb_livres")),
            safe_float(row.get("prix_moyen")),
            safe(row.get("source"), ""),
            safe(row.get("date_gold"), ""),
        ))
    conn.commit()
    print(f"[OK] distribution_notes chargee : {len(df)} lignes")

# ─── TABLE 3 : top_livres_chers ──────────────────────────────────────────────
def load_top_livres_chers():
    df = read_gold("top_livres_chers")
    if df.empty:
        return
    cursor.execute("TRUNCATE TABLE top_livres_chers")
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO top_livres_chers
                (categorie, rang, titre, auteur, prix,
                 note_etoiles, isbn, url, source, date_gold)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            safe(row.get("categorie"), "Unknown"),
            safe_int(row.get("rang")),
            safe(row.get("titre"), "")[:500],
            safe(row.get("auteur"), "")[:300],
            safe_float(row.get("prix")),
            safe_float(row.get("note_etoiles")),
            safe(row.get("isbn"), "")[:20],
            safe(row.get("url"), "")[:500],
            safe(row.get("source"), ""),
            safe(row.get("date_gold"), ""),
        ))
    conn.commit()
    print(f"[OK] top_livres_chers chargee : {len(df)} lignes")

# ─── TABLE 4 : kpi_par_auteur ────────────────────────────────────────────────
def load_kpi_auteur():
    df = read_gold("kpi_par_auteur")
    if df.empty:
        return
    cursor.execute("TRUNCATE TABLE kpi_par_auteur")
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO kpi_par_auteur
                (auteur, nb_livres, nb_avis_total,
                 annee_min, annee_max, source, date_gold)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (
            safe(row.get("auteur"), "Unknown")[:300],
            safe_int(row.get("nb_livres")),
            safe_int(row.get("nb_avis_total")),
            safe_float(row.get("annee_min")),
            safe_float(row.get("annee_max")),
            safe(row.get("source"), ""),
            safe(row.get("date_gold"), ""),
        ))
    conn.commit()
    print(f"[OK] kpi_par_auteur chargee : {len(df)} lignes")

# ─── TABLE 5 : livres_flat ────────────────────────────────────────────────────
def load_livres_flat():
    df = read_gold("livres_flat")
    if df.empty:
        return
    cursor.execute("TRUNCATE TABLE livres_flat")
    total = 0
    erreurs = 0
    for _, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT INTO livres_flat
                    (titre, auteur, prix, note_etoiles, categorie,
                     disponibilite, isbn, nb_avis, langue,
                     annee_publication, source, qualite_score,
                     date_scraping, url, date_gold)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                safe(row.get("titre"), "")[:500],
                safe(row.get("auteur"), "")[:300],
                safe_float(row.get("prix")),
                safe_float(row.get("note_etoiles")),
                safe(row.get("categorie"), "Unknown"),
                safe(row.get("disponibilite"), "unknown"),
                safe(row.get("isbn"), "")[:20],
                safe_int(row.get("nb_avis")),
                safe(row.get("langue"), "en")[:10],
                safe_float(row.get("annee_publication")),
                safe(row.get("source"), ""),
                safe_float(row.get("qualite_score")),
                safe(row.get("date_scraping"), ""),
                safe(row.get("url"), "")[:500],
                safe(row.get("date_gold"), ""),
            ))
            total += 1
        except Exception as e:
            erreurs += 1
            print(f"Erreur ligne : {e}")

    conn.commit()
    print(f"[OK] livres_flat chargee : {total} lignes ({erreurs} erreurs)")

# ─── LANCEMENT ────────────────────────────────────────────────────────────────
print("Chargement Gold → MySQL...")
print("="*50)

load_kpi_categorie()
load_distribution_notes()
load_top_livres_chers()
load_kpi_auteur()
load_livres_flat()

cursor.close()
conn.close()
print("="*50)
print("Termine !")