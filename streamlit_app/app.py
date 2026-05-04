import streamlit as st
import pandas as pd
import mysql.connector
import plotly.express as px

st.set_page_config(page_title="BookFlow Analytics", layout="wide")
st.title("📚 BookFlow — Dashboard Analytique")

# Connexion MySQL
@st.cache_resource
def get_conn():
    return mysql.connector.connect(
        host="mysql", port=3306,
        database="books_dw",
        user="bigdata", password="bigdata123"
    )

def load(query):
    conn = get_conn()
    return pd.read_sql(query, conn)

# ─── KPIs ────────────────────────────────────────────────────────────────────
df_kpi = load("SELECT * FROM kpi_par_categorie")
df_flat = load("SELECT * FROM livres_flat")
df_notes = load("SELECT * FROM distribution_notes")
df_top = load("SELECT * FROM top_livres_chers")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Livres", len(df_flat))
col2.metric("Prix Moyen", f"£{df_kpi['prix_moyen'].mean():.2f}")
col3.metric("Note Moyenne", f"{df_kpi['note_moyenne'].mean():.1f} ⭐")
col4.metric("Nb Genres", len(df_kpi))

st.divider()

# ─── GRAPHIQUES ──────────────────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Top Genres par Nb Livres")
    fig = px.bar(df_kpi.sort_values("nb_livres", ascending=True).tail(10),
                 x="nb_livres", y="categorie", orientation="h",
                 color="nb_livres", color_continuous_scale="Blues")
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Distribution des Notes")
    fig = px.pie(df_notes, values="nb_livres", names="note_etoiles",
                 color_discrete_sequence=px.colors.sequential.Blues)
    st.plotly_chart(fig, use_container_width=True)

col1, col2 = st.columns(2)

with col1:
    st.subheader("Prix Moyen par Genre")
    fig = px.bar(df_kpi.sort_values("prix_moyen", ascending=False).head(10),
                 x="categorie", y="prix_moyen", color="prix_moyen",
                 color_continuous_scale="Greens")
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Disponibilité")
    dispo = df_flat["disponibilite"].value_counts().reset_index()
    fig = px.pie(dispo, values="count", names="disponibilite")
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ─── TOP LIVRES ───────────────────────────────────────────────────────────────
st.subheader("🏆 Top Livres les Plus Chers")
st.dataframe(df_top[["titre", "auteur", "prix", "note_etoiles", "categorie"]].head(10))

st.divider()

# ─── CATALOGUE ────────────────────────────────────────────────────────────────
st.subheader("📖 Catalogue Complet")
genre = st.selectbox("Filtrer par genre", ["Tous"] + list(df_flat["categorie"].unique()))
if genre != "Tous":
    df_flat = df_flat[df_flat["categorie"] == genre]
st.dataframe(df_flat[["titre", "auteur", "prix", "note_etoiles", "categorie", "langue"]].head(100))