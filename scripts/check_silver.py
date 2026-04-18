
import boto3
import pandas as pd
import pyarrow.parquet as pq
import io

#minioconfig
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key=os.getenv('MINIO_ACCESS_KEY'),
)
BUCKET = 'datalake'

def read_all_parquets(prefix):
    frames = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if not key.endswith('.parquet'):
                continue
            body = s3.get_object(Bucket=BUCKET, Key=key)['Body'].read()
            df = pq.read_table(io.BytesIO(body)).to_pandas()
            frames.append(df)
            print(f"Lu : {key} ({len(df)} lignes)")
    if not frames:
        print(f"Aucun parquet trouvé sous : {prefix}")
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def check_source(nom, prefix):
    df = read_all_parquets(prefix)
    if df.empty:
        return

    total = len(df)

    # aperçu des 5 premières lignes
    cols_affichage = ['titre', 'auteur', 'prix', 'note_etoiles',
                      'categorie', 'langue', 'disponibilite',
                      'annee_publication', 'qualite_score']
    cols_dispo = [c for c in cols_affichage if c in df.columns]
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 120)
    pd.set_option('display.max_colwidth', 30)
    print(df[cols_dispo].head(5).to_string(index=False))

    #types des colonnes
    print(f"\n{'─'*30} TYPES DES COLONNES")
    print(df.dtypes.to_string())

    #valeurs nulles
    print(f"\n{'─'*30} VALEURS NULLES ({total} lignes total)")
    nulls = df.isnull().sum()
    nulls_pct = (nulls / total * 100).round(1)
    null_report = pd.DataFrame({'nulls': nulls, '%': nulls_pct})
    print(null_report[null_report['nulls'] > 0].to_string())




    # prix
    if 'prix' in df.columns:
        prix_ok = df['prix'].notna().sum()
        prix_negatif = (df['prix'].dropna() < 0).sum()
        print(f"  Prix renseigné    : {prix_ok}/{total}")
        print(f"  Prix négatifs     : {prix_negatif}  {'bien' if prix_negatif == 0 else ' PROBLÈME'}")

    # Auteur
    if 'auteur' in df.columns:
        unknown_restants = (df['auteur'] == 'Unknown').sum()
        print(f"  'Unknown' restants: {unknown_restants}  {'bien' if unknown_restants == 0 else 'nettoyage incomplet'}")

    # Langue
    if 'langue' in df.columns:
        valeurs_langue = df['langue'].value_counts().to_dict()
        trois_lettres = [v for v in df['langue'].dropna() if len(str(v)) == 3]
        print(f"  Langues présentes : {valeurs_langue}")
        print(f"  Codes 3 lettres   : {len(trois_lettres)}  {'' if len(trois_lettres) == 0 else ' normalisation incomplète'}")

    # Disponibilité
    if 'disponibilite' in df.columns:
        valeurs_dispo = df['disponibilite'].value_counts().to_dict()
        print(f"  Disponibilité     : {valeurs_dispo}")

    # Description dupliquée
    if 'description' in df.columns:
        def is_duped(text):
            if not text or pd.isna(text):
                return False
            half = len(text) // 2
            return half > 30 and text[half:half+20] == text[:20]
        dupes = df['description'].apply(is_duped).sum()
        print(f"  Descriptions dupliquées: {dupes}  {'bien' if dupes == 0 else 'encore dupliquées'}")

    # Score qualité
    if 'qualite_score' in df.columns:
        print(f"\n{'─'*30} SCORE QUALITÉ (0-100)")
        print(f"  Moyenne  : {df['qualite_score'].mean():.1f}")
        print(f"  Min      : {df['qualite_score'].min()}")
        print(f"  Max      : {df['qualite_score'].max()}")
        print(f"  Répartition :")
        bins = pd.cut(df['qualite_score'], bins=[0,20,40,60,80,100], include_lowest=True)
        print(bins.value_counts().sort_index().to_string())

    # stats par catégorie
    if 'categorie' in df.columns:
        print(f"\n{'─'*30} STATS PAR CATÉGORIE")
        agg = {'titre': 'count'}
        if 'prix' in df.columns:
            agg['prix'] = 'mean'
        if 'note_etoiles' in df.columns:
            agg['note_etoiles'] = 'mean'
        if 'qualite_score' in df.columns:
            agg['qualite_score'] = 'mean'
        stats = df.groupby('categorie').agg(agg).round(2)
        stats.columns = ['nb_livres'] + [c for c in stats.columns if c != 'titre']
        print(stats.to_string())

    print(f"\n vérification {nom} terminée — {total} livres Silver\n")


#lancer les verifications
check_source(
    nom    = "books_toscrape_com",
    prefix = "silver/books/books_toscrape_com/"
)

check_source(
    nom    = "openlibrary_org",
    prefix = "silver/books/openlibrary_org/"
)

