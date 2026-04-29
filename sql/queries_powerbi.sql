-- ─── KPIs GLOBAUX ─────────────────────────────────────────────────────────
SELECT
    COUNT(*)                            AS total_livres,
    ROUND(AVG(prix)::numeric, 2)        AS prix_moyen,
    ROUND(AVG(note_etoiles)::numeric,2) AS note_moyenne,
    COUNT(DISTINCT fk_source)           AS nb_sources,
    COUNT(DISTINCT fk_categorie)        AS nb_categories
FROM fact_scraping;

-- ─── TOP 10 GENRES ────────────────────────────────────────────────────────
SELECT
    c.nom_categorie,
    COUNT(*)                            AS nb_livres,
    ROUND(AVG(f.prix)::numeric, 2)      AS prix_moyen,
    ROUND(AVG(f.note_etoiles)::numeric,2) AS note_moyenne
FROM fact_scraping f
JOIN dim_categorie c ON f.fk_categorie = c.id_categorie
GROUP BY c.nom_categorie
ORDER BY nb_livres DESC
LIMIT 10;

-- ─── COMPARAISON SOURCES ──────────────────────────────────────────────────
SELECT
    s.nom_site,
    COUNT(*)                              AS nb_livres,
    ROUND(AVG(f.prix)::numeric, 2)        AS prix_moyen,
    ROUND(AVG(f.note_etoiles)::numeric,2) AS note_moyenne,
    ROUND(100.0 * SUM(CASE WHEN d.est_disponible THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_dispo
FROM fact_scraping f
JOIN dim_source s ON f.fk_source = s.id_source
JOIN dim_disponibilite d ON f.fk_dispo = d.id_dispo
GROUP BY s.nom_site;

-- ─── DISTRIBUTION PRIX ────────────────────────────────────────────────────
SELECT
    CASE
        WHEN prix < 20  THEN 'Moins de 20'
        WHEN prix < 40  THEN '20 a 40'
        WHEN prix < 60  THEN '40 a 60'
        ELSE 'Plus de 60'
    END AS tranche_prix,
    COUNT(*) AS nb_livres,
    ROUND(AVG(note_etoiles)::numeric, 2) AS note_moyenne
FROM fact_scraping
WHERE prix IS NOT NULL
GROUP BY tranche_prix
ORDER BY tranche_prix;

-- ─── TOP 10 LIVRES MIEUX NOTES ────────────────────────────────────────────
SELECT
    l.titre, l.auteur,
    f.prix, f.note_etoiles, f.nb_avis,
    c.nom_categorie,
    s.nom_site
FROM fact_scraping f
JOIN dim_livre l      ON f.fk_livre     = l.id_livre
JOIN dim_categorie c  ON f.fk_categorie = c.id_categorie
JOIN dim_source s     ON f.fk_source    = s.id_source
WHERE f.note_etoiles IS NOT NULL
ORDER BY f.note_etoiles DESC, f.nb_avis DESC
LIMIT 10;

-- ─── EVOLUTION TEMPORELLE ─────────────────────────────────────────────────
SELECT
    dt.date,
    s.nom_site,
    COUNT(*) AS nb_livres
FROM fact_scraping f
JOIN dim_date dt ON f.fk_date = dt.id_date
JOIN dim_source s ON f.fk_source = s.id_source
GROUP BY dt.date, s.nom_site
ORDER BY dt.date DESC;