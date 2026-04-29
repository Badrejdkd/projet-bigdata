import json
import re
import time
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
from datetime import datetime, timezone
from botocore.exceptions import ClientError

#minio config
MINIO_CONFIG = dict(
    endpoint_url         = 'http://localhost:9000',
    aws_access_key_id    = 'minioadmin',
    aws_secret_access_key= os.getenv('MINIO_ACCESS_KEY'),
)
BUCKET              = 'datalake'
now_str             = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
CATEGORIES_INVALIDES= {'Add a comment', 'Add_a_comment', 'default', 'Default', ''}
MAX_RETRIES         = 3 

#colonnes minimales
COLONNES_REQUISES = {
    'titre': None, 'auteur': None, 'prix': None,
    'note_etoiles': None, 'disponibilite': 'unknown',
    'categorie': 'Unknown', 'description': None,
    'isbn': None, 'nb_avis': 0, 'langue': None,
    'annee_publication': None, 'url': None,
    'source': None, 'date_scraping': None,
}

# CONNEXION MINIO AVEC RETRY
def get_s3_client():
    return boto3.client('s3', **MINIO_CONFIG)

def s3_retry(fn, *args, **kwargs):
    """Exécute une opération S3 avec retry automatique (3 tentatives)."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except ClientError as e:
            if attempt == MAX_RETRIES:
                raise
            print(f"MinIO erreur (tentative {attempt}/{MAX_RETRIES}) : {e} — retry dans 2s...")
            time.sleep(2)

s3 = get_s3_client()

#lire tous les JSON Bronze
def read_bronze_jsons(prefix):
    """
    Lit tous les .json sous un préfixe MinIO.
    - Skippe les lignes JSON invalides sans crasher
    - Ajoute les colonnes manquantes si le schema a changé
    """
    rows = []
    skipped_lines  = 0
    skipped_files  = 0

    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=BUCKET, Prefix=prefix)

    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            if not key.endswith('.json'):
                continue
            try:
                body = s3_retry(
                    s3.get_object, Bucket=BUCKET, Key=key
                )['Body'].read().decode('utf-8')
            except Exception as e:
                print(f"Impossible de lire {key} : {e} — skippé")
                skipped_files += 1
                continue

            for line in body.strip().split('\n'):
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    skipped_lines += 1

    if skipped_lines > 0:
        print(f" {skipped_lines} ligne(s) JSON invalide(s) ignorée(s)")
    if skipped_files > 0:
        print(f"{skipped_files} fichier(s) illisible(s) ignoré(s)")
    if not rows:
        print(f"Aucune donnée trouvée sous : {prefix}")
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # ── Gestion schema flottant ──────────────────────────────
    # Si le scraper a ajouté de nouvelles colonnes → pas de crash
    # Si des colonnes sont manquantes → on les crée avec valeur par défaut
    for col, default in COLONNES_REQUISES.items():
        if col not in df.columns:
            print(f"Colonne '{col}' absente du Bronze → ajoutée avec valeur par défaut")
            df[col] = default

    print(f"{len(rows)} lignes lues depuis {prefix}")
    return df

# ecrire Parquet Silver (par cat)
def write_silver_parquet(df, prefix):
    """
    Écrase uniquement les fichiers par catégorie qu'on réécrit.
    Les autres catégories Silver existantes ne sont pas touchées.
    """
    if df.empty:
        print("DataFrame vide, rien à écrire.")
        return
    for categorie, groupe in df.groupby('categorie'):
        cat_clean = str(categorie).replace(' ', '_').replace('/', '_')
        key = f"{prefix}categorie={cat_clean}/data.parquet"
        try:
            s3_retry(s3.delete_object, Bucket=BUCKET, Key=key)
        except Exception:
            pass
        buffer = io.BytesIO()
        pq.write_table(pa.Table.from_pandas(groupe.reset_index(drop=True)), buffer)
        buffer.seek(0)
        s3_retry(s3.put_object, Bucket=BUCKET, Key=key,
                 Body=buffer.read())
        print(f"Silver : {key} ({len(groupe)} lignes)")

#mettre en quarantaine les lignes invalides

def write_quarantine(df, source_name):
    """
    Envoie les lignes invalides dans datalake/quarantine/
    pour pouvoir les analyser plus tard sans les perdre.
    """
    if df.empty:
        return
    key = f"quarantine/{source_name}/{now_str.replace(':', '-')}.json"
    body = df.to_json(orient='records', lines=True, force_ascii=False)
    s3_retry(s3.put_object, Bucket=BUCKET, Key=key,
             Body=body.encode('utf-8'))
    print(f"Quarantaine : {len(df)} ligne(s) → {key}")

#transformation

def clean_prix(val):
    if pd.isna(val) or str(val).strip() == '':
        return None
    cleaned = re.sub(r'[^0-9.]', '', str(val))
    try:
        f = float(cleaned)
        return f if f >= 0 else None
    except ValueError:
        return None

def clean_auteur(val):
    if pd.isna(val) or str(val).strip().lower() in ('unknown', '', ' '):
        return None
    return str(val).strip()

def dedup_description(text):
    if not text or pd.isna(text):
        return None
    text = str(text).strip()
    half = len(text) // 2
    if half > 30 and text[half:half+20].strip() == text[:20].strip():
        return text[:half].strip()
    return text

def clean_description_ol(text):
    if not text or pd.isna(text):
        return None
    return re.sub(r'[\r\n]+', ' ', str(text)).strip()

LANG_MAP = {
    'eng': 'en', 'fre': 'fr', 'spa': 'es', 'ger': 'de',
    'ita': 'it', 'por': 'pt', 'ara': 'ar', 'jpn': 'ja',
    'chi': 'zh', 'rus': 'ru', 'dut': 'nl', 'kor': 'ko'
}
def normalize_langue(val):
    if pd.isna(val) or val is None:
        return None
    val = str(val).strip().lower()
    return LANG_MAP.get(val, val[:2] if len(val) == 3 else val)

def normalize_disponibilite(val):
    if pd.isna(val) or val is None:
        return 'unknown'
    val = str(val).lower()
    if 'in stock' in val:   return 'available'
    if 'out' in val:        return 'unavailable'
    return 'unknown'

def clean_isbn(val):
    if pd.isna(val) or str(val).strip() == '':
        return None
    return str(val).strip()

def fix_categorie(val):
    if pd.isna(val) or str(val).strip() in CATEGORIES_INVALIDES:
        return 'Unknown'
    return str(val).strip()

def qualite_score(row):
    score = 0
    score += 20 if pd.notna(row.get('titre'))            and row.get('titre')  else 0
    score += 20 if pd.notna(row.get('auteur'))            and row.get('auteur') else 0
    score += 20 if pd.notna(row.get('prix'))                                    else 0
    score += 20 if pd.notna(row.get('note_etoiles'))                            else 0
    score += 10 if pd.notna(row.get('annee_publication'))                       else 0
    score += 10 if pd.notna(row.get('isbn'))              and row.get('isbn')   else 0
    return score

#PIPELINE PRINCIPAL
def process_source(source_name, bronze_prefix, silver_prefix,
                   clean_fn, dedup_key):
    """
    Pipeline complet pour une source :
    1. Lit tout le Bronze (tous les scraping passés)
    2. Déduplique (garde le plus récent par dedup_key)
    3. Quarantaine les lignes invalides (sans titre)
    4. Applique les transformations
    5. Écrit le Silver (écrase uniquement les fichiers concernés)
    """
    print(f"Bronze → Silver — {source_name}")
    

    df = read_bronze_jsons(bronze_prefix)
    if df.empty:
        return

    total_brut = len(df)

   
    #si le meme livre a été scrapé plusieurs fois, on garde uniquement la version la plus récente
    if dedup_key in df.columns and 'date_scraping' in df.columns:
        avant = len(df)
        df['date_scraping'] = pd.to_datetime(df['date_scraping'], errors='coerce')
        df = df.sort_values('date_scraping', ascending=False)
        df = df.drop_duplicates(subset=[dedup_key], keep='first')
        df['date_scraping'] = df['date_scraping'].astype(str)
        nb_doublons = avant - len(df)
        if nb_doublons > 0:
            print(f"Déduplication sur '{dedup_key}' : {nb_doublons} doublon(s) supprimé(s)")
    else:
        print(f"Clé de déduplication '{dedup_key}' absente — pas de déduplication")

    #QUARANTAINE
    # Lignes sans titre = données corrompues ou incomplètes
    mask_invalide = df['titre'].isna() | (df['titre'].str.strip() == '')
    df_quarantine = df[mask_invalide].copy()
    df = df[~mask_invalide].copy()

    if not df_quarantine.empty:
        write_quarantine(df_quarantine, source_name)

    if df.empty:
        print("Plus aucune ligne valide après quarantaine !")
        return

    #TRANSFORMATIONS
    df = clean_fn(df)


    df['qualite_score'] = df.apply(qualite_score, axis=1)
    df['date_silver']   = now_str
    df['source_layer']  = 'silver'

    
    print(f"\n   Rapport {source_name} :")
    print(f"     Lignes Bronze lues       : {total_brut}")
    print(f"     Après déduplication      : {len(df) + len(df_quarantine)}")
    print(f"     Mis en quarantaine       : {len(df_quarantine)}")
    print(f"     Lignes Silver écrites    : {len(df)}")
    print(f"     Score qualité moyen      : {df['qualite_score'].mean():.1f}/100")
    print(f"     Catégories              : {sorted(df['categorie'].unique())}")

    #ecriture SILVER
    write_silver_parquet(df, silver_prefix)


#transformation par source
def clean_books_toscrape(df):
    df['prix']          = df['prix'].apply(clean_prix)
    df['auteur']        = df['auteur'].apply(clean_auteur)
    df['description']   = df['description'].apply(dedup_description)
    df['disponibilite'] = df['disponibilite'].apply(normalize_disponibilite)
    df['isbn']          = df['isbn'].apply(clean_isbn)
    df['langue']        = df.get('langue', pd.Series(['en']*len(df))).fillna('en')
    df['categorie']     = df['categorie'].apply(fix_categorie)
    return df

def clean_openlibrary(df):
    df['prix']          = None
    df['auteur']        = df['auteur'].apply(clean_auteur)
    df['description']   = df['description'].apply(clean_description_ol)
    df['disponibilite'] = df['disponibilite'].apply(normalize_disponibilite)
    df['isbn']          = df['isbn'].apply(clean_isbn)
    df['langue']        = df['langue'].apply(normalize_langue)
    df['categorie']     = df['categorie'].apply(fix_categorie)
    return df


#lancement

process_source(
    source_name   = 'books_toscrape_com',
    bronze_prefix = 'bronze/books/books_toscrape_com/',
    silver_prefix = 'silver/books/books_toscrape_com/',
    clean_fn      = clean_books_toscrape,
    dedup_key     = 'url',          
)

process_source(
    source_name   = 'openlibrary_org',
    bronze_prefix = 'bronze/books/openlibrary_org/',
    silver_prefix = 'silver/books/openlibrary_org/',
    clean_fn      = clean_openlibrary,
    dedup_key     = 'url',      
)


