import mysql.connector

conn = mysql.connector.connect(
    host="localhost",
    port=3307,
    database="books_dw",
    user="bigdata",
    password="bigdata123"
)
cursor = conn.cursor()

print("Tests de qualite...")
erreurs = 0

tests = [
    ("Livres sans titre",
     "SELECT COUNT(*) FROM dim_livre WHERE titre IS NULL OR titre = ''", 0),
    ("Prix negatifs",
     "SELECT COUNT(*) FROM fact_scraping WHERE prix < 0", 0),
    ("Notes hors plage",
     "SELECT COUNT(*) FROM fact_scraping WHERE note_etoiles NOT BETWEEN 1 AND 5", 0),
    ("URLs manquantes",
     "SELECT COUNT(*) FROM dim_livre WHERE url IS NULL OR url = ''", 0),
]

for nom, requete, seuil in tests:
    cursor.execute(requete)
    resultat = cursor.fetchone()[0]
    statut = "OK" if resultat <= seuil else "ECHEC"
    print(f"[{statut}] {nom} : {resultat}")
    if statut == "ECHEC":
        erreurs += 1

cursor.execute("SELECT COUNT(*) FROM fact_scraping")
print(f"\nTotal faits         : {cursor.fetchone()[0]}")

cursor.execute("SELECT COUNT(*) FROM dim_livre")
print(f"Total livres        : {cursor.fetchone()[0]}")

cursor.execute("SELECT ROUND(AVG(qualite_score), 2) FROM fact_scraping")
print(f"Score qualite moyen : {cursor.fetchone()[0]}/100")

print(f"Tests echoues       : {erreurs}")

cursor.close()
conn.close()