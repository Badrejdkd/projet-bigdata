import mysql.connector

conn = mysql.connector.connect(
    host="localhost",
    port=3307,
    database="books_dw",
    user="bigdata",
    password="bigdata123"
)
cursor = conn.cursor()

print("Creation du schema MySQL base sur l'architecture Gold...")

tables = [

    # TABLE 1 : kpi_par_categorie ─────────────────────────────────────────
    # Depuis gold/analytics/kpi_par_categorie/data.parquet
    # Colonnes : categorie, nb_livres, prix_moyen, prix_min, prix_max,
    #            note_moyenne, total_avis, source, date_gold
    """
    CREATE TABLE IF NOT EXISTS kpi_par_categorie (
        id              INT AUTO_INCREMENT PRIMARY KEY,
        categorie       VARCHAR(100),
        nb_livres       INT,
        prix_moyen      FLOAT,
        prix_min        FLOAT,
        prix_max        FLOAT,
        note_moyenne    FLOAT,
        total_avis      INT,
        source          VARCHAR(100),
        date_gold       VARCHAR(30)
    )
    """,

    # TABLE 2 : distribution_notes
    # Depuis gold/analytics/distribution_notes/data.parquet
    # Colonnes : note_etoiles, categorie, nb_livres, prix_moyen,
    #            source, date_gold
    """
    CREATE TABLE IF NOT EXISTS distribution_notes (
        id              INT AUTO_INCREMENT PRIMARY KEY,
        note_etoiles    FLOAT,
        categorie       VARCHAR(100),
        nb_livres       INT,
        prix_moyen      FLOAT,
        source          VARCHAR(100),
        date_gold       VARCHAR(30)
    )
    """,

    #TABLE 3 : top_livres_chers
    # Depuis gold/analytics/top_livres_chers/data.parquet
    # Colonnes : categorie, rang, titre, auteur, prix, note_etoiles,
    #            isbn, url, source, date_gold
    """
    CREATE TABLE IF NOT EXISTS top_livres_chers (
        id              INT AUTO_INCREMENT PRIMARY KEY,
        categorie       VARCHAR(100),
        rang            INT,
        titre           VARCHAR(500),
        auteur          VARCHAR(300),
        prix            FLOAT,
        note_etoiles    FLOAT,
        isbn            VARCHAR(20),
        url             VARCHAR(500),
        source          VARCHAR(100),
        date_gold       VARCHAR(30)
    )
    """,

    # TABLE 4 : kpi_par_auteur
    # Depuis gold/analytics/kpi_par_auteur/data.parquet
    # Colonnes : auteur, nb_livres, nb_avis_total, annee_min, annee_max,
    #            source, date_gold
    """
    CREATE TABLE IF NOT EXISTS kpi_par_auteur (
        id              INT AUTO_INCREMENT PRIMARY KEY,
        auteur          VARCHAR(300),
        nb_livres       INT,
        nb_avis_total   INT,
        annee_min       FLOAT,
        annee_max       FLOAT,
        source          VARCHAR(100),
        date_gold       VARCHAR(30)
    )
    """,

    # TABLE 5 : livres_flat 
    # Depuis gold/analytics/livres_flat/data.parquet
    # Colonnes : titre, auteur, prix, note_etoiles, categorie,
    #            disponibilite, isbn, nb_avis, langue,
    #            annee_publication, source, qualite_score,
    #            date_scraping, url, date_gold
    """
    CREATE TABLE IF NOT EXISTS livres_flat (
        id                  INT AUTO_INCREMENT PRIMARY KEY,
        titre               VARCHAR(500),
        auteur              VARCHAR(300),
        prix                FLOAT,
        note_etoiles        FLOAT,
        categorie           VARCHAR(100),
        disponibilite       VARCHAR(50),
        isbn                VARCHAR(20),
        nb_avis             INT,
        langue              VARCHAR(10),
        annee_publication   FLOAT,
        source              VARCHAR(100),
        qualite_score       FLOAT,
        date_scraping       VARCHAR(50),
        url                 VARCHAR(500),
        date_gold           VARCHAR(30)
    )
    """,
]

for sql in tables:
    nom = [l.strip() for l in sql.strip().split("\n") if "CREATE TABLE" in l][0]
    cursor.execute(sql)
    print(f"[OK] {nom}")

conn.commit()
cursor.close()
conn.close()
print("\nSchema MySQL cree avec succes !")