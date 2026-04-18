import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
from datetime import datetime, timezone
#minio config
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key=os.getenv('MINIO_ACCESS_KEY'),
)
BUCKET  = 'datalake'
now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def read_all_parquets(prefix):
    """Lit tous les .parquet sous un préfixe MinIO → DataFrame."""
    frames = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if not key.endswith('.parquet'):
                continue
            body = s3.get_object(Bucket=BUCKET, Key=key)['Body'].read()
            frames.append(pq.read_table(io.BytesIO(body)).to_pandas())
    if not frames:
        print(f"Aucun parquet trouvé sous : {prefix}")
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def delete_prefix(prefix):
    """Supprime tous les objets sous un préfixe MinIO."""
    paginator = s3.get_paginator('list_objects_v2')
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            keys.append({'Key': obj['Key']})
    if keys:
        s3.delete_objects(Bucket=BUCKET, Delete={'Objects': keys})
        print(f" {len(keys)} ancien(s) fichier(s) supprimé(s)")


def write_gold(df, table_name):
    """Écrit un DataFrame Gold en Parquet dans MinIO.
    Remplace uniquement le fichier de cette table (pas tout le Gold)."""
    if df.empty:
        print(f"Table {table_name} vide, rien à écrire.")
        return
    key = f"gold/analytics/{table_name}/data.parquet"

    #supp uniquement CE fichier avant de réécrire
    try:
        s3.delete_object(Bucket=BUCKET, Key=key)
    except Exception:
        pass 
    buffer = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(df.reset_index(drop=True)), buffer)
    buffer.seek(0)
    s3.put_object(Bucket=BUCKET, Key=key, Body=buffer.read())
    print(f"Gold écrit : {key} ({len(df)} lignes)")


#lecture SILVER
print("Lecture Silver")

df_bts = read_all_parquets('silver/books/books_toscrape_com/')
df_ol  = read_all_parquets('silver/books/openlibrary_org/')

print(f"  books_toscrape_com : {len(df_bts)} lignes")
print(f"  openlibrary_org    : {len(df_ol)} lignes")


# TABLE GOLD 1 — KPI PAR CATÉGORIE (books_toscrape_com)
# 1 ligne par catégorie : nb livres, prix moyen/min/max, note moyenne
# Utilisé pour : les cartes KPI et graphiques barres dans Superset
print("\n" + "="*60)
print("📊 Gold 1 — KPI par catégorie (books_toscrape_com)")
print("="*60)

if not df_bts.empty:
    kpi = df_bts.groupby('categorie').agg(
        nb_livres    =('titre',        'count'),
        prix_moyen   =('prix',         'mean'),
        prix_min     =('prix',         'min'),
        prix_max     =('prix',         'max'),
        note_moyenne =('note_etoiles', 'mean'),
        total_avis   =('nb_avis',      'sum'),
    ).round(2).reset_index()

    kpi['source']     = 'books_toscrape_com'
    kpi['date_gold']  = now_str

    # Trier par nb_livres décroissant
    kpi = kpi.sort_values('nb_livres', ascending=False)

    print(kpi[['categorie', 'nb_livres', 'prix_moyen', 'note_moyenne']].to_string(index=False))
    write_gold(kpi, 'kpi_par_categorie')


# TABLE GOLD 2 — DISTRIBUTION DES NOTES (books_toscrape_com)
# Nb livres par note (1★ à 5★) et par catégorie
# Utilisé pour : graphique barres empilées dans Superset

if not df_bts.empty:
    distrib = df_bts.groupby(['note_etoiles', 'categorie']).agg(
        nb_livres  =('titre', 'count'),
        prix_moyen =('prix',  'mean'),
    ).round(2).reset_index()

    distrib['source']    = 'books_toscrape_com'
    distrib['date_gold'] = now_str
    distrib = distrib.sort_values(['categorie', 'note_etoiles'])

    print(distrib.to_string(index=False))
    write_gold(distrib, 'distribution_notes')


# TABLE GOLD 3 — TOP 10 LIVRES LES PLUS CHERS (books_toscrape_com)
# Par catégorie, les livres les plus chers avec leur rang
# Utilisé pour : tableau classement dans Superset


if not df_bts.empty:
    # Rang par catégorie selon prix décroissant
    df_bts['rang'] = df_bts.groupby('categorie')['prix'] \
                            .rank(method='first', ascending=False).astype(int)

    top10 = df_bts[df_bts['rang'] <= 10][[
        'categorie', 'rang', 'titre', 'auteur',
        'prix', 'note_etoiles', 'isbn', 'url'
    ]].copy()

    top10['source']    = 'books_toscrape_com'
    top10['date_gold'] = now_str
    top10 = top10.sort_values(['categorie', 'rang'])

    print(top10[['categorie', 'rang', 'titre', 'prix']].to_string(index=False))
    write_gold(top10, 'top_livres_chers')


# TABLE GOLD 4 — KPI PAR AUTEUR (openlibrary_org)
# Stats groupées par auteur : nb livres, années min/max, nb avis
# Utilisé pour : classement auteurs dans Superset


if not df_ol.empty:
    df_ol_with_auteur = df_ol[df_ol['auteur'].notna()]

    kpi_auteur = df_ol_with_auteur.groupby('auteur').agg(
        nb_livres      =('titre',             'count'),
        nb_avis_total  =('nb_avis',           'sum'),
        annee_min      =('annee_publication', 'min'),
        annee_max      =('annee_publication', 'max'),
    ).reset_index()

    kpi_auteur['source']    = 'openlibrary_org'
    kpi_auteur['date_gold'] = now_str
    kpi_auteur = kpi_auteur.sort_values('nb_avis_total', ascending=False)

    print(kpi_auteur.to_string(index=False))
    write_gold(kpi_auteur, 'kpi_par_auteur')


# TABLE GOLD 5 — VUE PLATE COMPLÈTE (les deux sources)
# Tous les livres des 2 sources, sans la description
# Utilisé pour : table principale Superset, exports CSV

print("\n")
print("Gold 5 — Vue plate complète (toutes sources)")

cols = ['titre', 'auteur', 'prix', 'note_etoiles', 'categorie',
        'disponibilite', 'isbn', 'nb_avis', 'langue',
        'annee_publication', 'source', 'qualite_score',
        'date_scraping', 'url']

frames = []
if not df_bts.empty:
    frames.append(df_bts[[c for c in cols if c in df_bts.columns]])
if not df_ol.empty:
    frames.append(df_ol[[c for c in cols if c in df_ol.columns]])

if frames:
    livres_flat = pd.concat(frames, ignore_index=True)
    livres_flat['date_gold'] = now_str

    print(f"  Total toutes sources : {len(livres_flat)} livres")
    print(f"  books_toscrape_com   : {(livres_flat['source'] == 'books.toscrape.com').sum()}")
    print(f"  openlibrary_org      : {(livres_flat['source'] == 'openlibrary.org').sum()}")

    write_gold(livres_flat, 'livres_flat')

