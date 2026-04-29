import mysql.connector

conn = mysql.connector.connect(
    host="localhost",
    port=3307,
    database="books_dw",
    user="bigdata",
    password="bigdata123"
)
cursor = conn.cursor()

print("Calcul des tables Gold...")

# agg_genre_stats
cursor.execute("TRUNCATE TABLE agg_genre_stats")
cursor.execute("""
    INSERT INTO agg_genre_stats
        (genre, nb_livres, prix_moyen, prix_min, prix_max, note_moyenne, pct_disponible)
    SELECT
        c.nom_categorie,
        COUNT(f.id),
        ROUND(AVG(f.prix), 2),
        MIN(f.prix),
        MAX(f.prix),
        ROUND(AVG(f.note_etoiles), 2),
        ROUND(100.0 * SUM(CASE WHEN d.est_disponible THEN 1 ELSE 0 END) / COUNT(f.id), 2)
    FROM fact_scraping f
    JOIN dim_categorie c ON f.fk_categorie = c.id_categorie
    JOIN dim_disponibilite d ON f.fk_dispo = d.id_dispo
    GROUP BY c.nom_categorie
""")
print("[OK] agg_genre_stats calculee")

# agg_daily_volumes
cursor.execute("TRUNCATE TABLE agg_daily_volumes")
cursor.execute("""
    INSERT INTO agg_daily_volumes (date_val, source, nb_livres)
    SELECT
        dt.date_val,
        s.nom_site,
        COUNT(f.id)
    FROM fact_scraping f
    JOIN dim_date dt ON f.fk_date = dt.id_date
    JOIN dim_source s ON f.fk_source = s.id_source
    GROUP BY dt.date_val, s.nom_site
""")
print("[OK] agg_daily_volumes calculee")

conn.commit()
cursor.close()
conn.close()
print("Tables Gold terminees !")