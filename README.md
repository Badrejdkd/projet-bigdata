# projet-bigdata
Ce projet consiste à concevoir et implémenter un pipeline Big Data complet dédié à la collecte, au stockage, à la transformation et à la visualisation de données livresques issues de plusieurs sources web.
## 🚀 Installation et lancement

### Prérequis
- Docker Desktop installé et démarré
- Python 3.10+
- Power BI Desktop

### 1. Cloner le projet
```bash
git clone https://github.com/votre-repo/projet-bigdata.git
cd projet-bigdata
```

### 2. Installer les dépendances Python
```bash
pip install scrapy boto3 kafka-python redis psycopg2-binary minio
```

### 3. Lancer l'infrastructure Docker
```bash
docker-compose up -d
```

### 4. Vérifier que tout tourne
```bash
docker ps
```

### 5. Initialiser les buckets MinIO
```bash
python scripts/init_minio.py
```

### 6. Lancer les scrapers
```bash
# Terminal 1
scrapy crawl books_toscrape

# Terminal 2
scrapy crawl books_openlibrary

# Terminal 3
scrapy crawl books_google
```

### 7. Charger les données dans PostgreSQL
```bash
python scripts/load_to_postgres.py
```

---

## 🌐 Interfaces disponibles

| Interface | URL | Login | Password |
|---|---|---|---|
| MinIO Console | http://localhost:9001 | minioadmin | minioadmin123 |
| Airflow | http://localhost:8080 | admin | admin123 |
| pgAdmin | http://localhost:5050 | admin@admin.com | admin123 |

---

## 🗄️ Sources de données

| Source | URL | Données |
|---|---|---|
| Books to Scrape | books.toscrape.com | Prix, notes, disponibilité |
| Open Library | openlibrary.org | Métadonnées, auteurs, langues |
| Google Books | googleapis.com | Notes, avis, descriptions |

---

## 📊 Architecture Médaillon

| Couche | Format | Contenu |
|---|---|---|
| Bronze | JSON | Données brutes du scraper |
| Silver | Parquet | Données nettoyées et normalisées |
| Gold | Parquet | Agrégations et KPI |

---

## 👥 Équipe

| Personne | Rôle |
|---|---|
| Personne 1 | Data Engineer — Ingestion & Infrastructure |
| Personne 2 | Data Engineer — Transformation & Orchestration |
| Personne 3 | Data Analyst — BI & Gouvernance |

---

## 📅 Informations

- **Filière :** IADATA 4ème année
- **École :** EMSI Casablanca
- **Année :** 2025/2026
- **Date limite :** 10 Mai 2026
