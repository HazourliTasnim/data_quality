"""
POC - Détection d'erreurs par Embeddings
Pipeline en 4 étapes : Fetch API -> Embedding -> Stockage Snowflake -> Détection d'erreurs
"""

import streamlit as st
import pandas as pd
import json
import numpy as np
from pathlib import Path
from datetime import datetime
import sys
import os

# Add native-app to path
SCRIPT_DIR = Path(__file__).parent.resolve()
NATIVE_APP_DIR = SCRIPT_DIR / "native-app"
DATA_DIR = SCRIPT_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

sys.path.insert(0, str(NATIVE_APP_DIR))

st.set_page_config(page_title="POC Data Quality", layout="wide")
st.title("POC - Détection d'erreurs par Embeddings")
st.caption("On récupère les données de l'API, on les vectorise, on les stocke dans Snowflake, puis on teste la détection d'erreurs par comparaison sémantique.")

# ============================================================================
# ÉTAPE 1 : Fetch des données depuis l'API
# ============================================================================
st.header("Étape 1 -- Fetch des données depuis l'API")
st.markdown(
    "On récupère toutes les données disponibles de l'API (NOM, TVA, SIRET, SIREN, secteur, forme juridique, etc.)."
)

col1, col2 = st.columns(2)
with col1:
    if st.button("Lancer le fetch API"):
        try:
            from fetch_api_data import create_sample_data, save_to_csv, save_to_json

            st.info("Interrogation de l'API entreprise (INSEE SIRENE)...")
            companies_data = create_sample_data()

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_file = str(DATA_DIR / f"entreprises_data_{timestamp}.csv")
            json_file = str(DATA_DIR / f"entreprises_data_{timestamp}.json")

            save_to_csv(companies_data, csv_file)
            save_to_json(companies_data, json_file)

            st.success(f"{len(companies_data)} entreprises récupérées avec succès.")
            st.dataframe(pd.DataFrame(companies_data))

            # Save to session state
            st.session_state.companies_data = companies_data
            st.session_state.data_file = json_file

        except Exception as e:
            st.error(f"Erreur lors du fetch : {str(e)}")

with col2:
    # Load existing data
    json_files = sorted(DATA_DIR.glob("entreprises_data_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if json_files:
        latest = json_files[0]
        with open(latest) as f:
            data = json.load(f)
        st.success(f"Fichier existant chargé : {latest.name}")
        st.dataframe(pd.DataFrame(data))

        if st.button("Utiliser ces données"):
            st.session_state.companies_data = data
            st.session_state.data_file = str(latest)

# ============================================================================
# ÉTAPE 2 : Embedding des données
# ============================================================================
st.header("Étape 2 -- Embedding des données")
st.markdown(
    "On vectorise les données en utilisant un modèle d'embedding (all-MiniLM-L6-v2). "
    "L'embedding transforme chaque entreprise en un vecteur numérique que le LLM peut comprendre et comparer."
)

if st.button("Générer les embeddings"):
    if "companies_data" not in st.session_state or not st.session_state.companies_data:
        st.warning("Aucune donnée disponible. Lance d'abord le fetch (étape 1).")
    else:
        try:
            from embedding_step import create_embeddings, save_embeddings

            with st.spinner("Vectorisation en cours (chargement du modèle all-MiniLM-L6-v2)..."):
                companies_data = st.session_state.companies_data

                embeddings_data, model = create_embeddings(companies_data)

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                embedding_file = str(DATA_DIR / f"entreprises_embeddings_{timestamp}.json")

                save_embeddings(embeddings_data, embedding_file)

                st.success(f"Embeddings générés pour {len(embeddings_data)} entreprises.")
                st.session_state.embeddings_data = embeddings_data
                st.session_state.embeddings_file = embedding_file

                # Show summary
                col_a, col_b, col_c = st.columns(3)
                with col_a:
                    st.metric("Dimension du vecteur", 384)
                with col_b:
                    st.metric("Entreprises", len(embeddings_data))
                with col_c:
                    st.metric("Modèle", "MiniLM-L6-v2")

        except Exception as e:
            st.error(f"Erreur lors de l'embedding : {str(e)}")

# ============================================================================
# ÉTAPE 3 : Stockage dans Snowflake
# ============================================================================
st.header("Étape 3 -- Stockage dans Snowflake")
st.markdown(
    "On stocke ces vecteurs dans une base de données Snowflake, "
    "et on lie cette BDD au LLM pour qu'il puisse interroger les embeddings de référence."
)

col1, col2 = st.columns(2)

with col1:
    snowflake_config = {
        'user': st.text_input("Utilisateur Snowflake", type="password", key="sf_user"),
        'password': st.text_input("Mot de passe", type="password", key="sf_pass"),
        'account': st.text_input("Account Snowflake", key="sf_account"),
        'warehouse': st.text_input("Warehouse", value="COMPUTE_WH", key="sf_warehouse"),
        'database': st.text_input("Database", key="sf_database"),
        'schema': st.text_input("Schema", value="PUBLIC", key="sf_schema"),
    }

with col2:
    if st.button("Envoyer vers Snowflake"):
        if any(not v for v in snowflake_config.values() if v not in ("COMPUTE_WH", "PUBLIC")):
            st.warning("Remplis tous les champs de connexion Snowflake.")
        elif "embeddings_data" not in st.session_state:
            st.warning("Génère d'abord les embeddings (étape 2).")
        else:
            try:
                from snowflake_storage_step import get_snowflake_connection, create_embedding_table, insert_embeddings_to_snowflake

                st.info("Connexion à Snowflake en cours...")
                os.environ.update({
                    'SNOWFLAKE_USER': snowflake_config['user'],
                    'SNOWFLAKE_PASSWORD': snowflake_config['password'],
                    'SNOWFLAKE_ACCOUNT': snowflake_config['account'],
                    'SNOWFLAKE_WAREHOUSE': snowflake_config['warehouse'],
                    'SNOWFLAKE_DATABASE': snowflake_config['database'],
                    'SNOWFLAKE_SCHEMA': snowflake_config['schema'],
                })

                conn = get_snowflake_connection()
                if conn:
                    create_embedding_table(conn)
                    insert_embeddings_to_snowflake(conn, st.session_state.embeddings_data)
                    conn.close()
                    st.success("Données envoyées dans Snowflake avec succès.")
                else:
                    st.error("Impossible de se connecter à Snowflake.")
            except Exception as e:
                st.error(f"Erreur Snowflake : {str(e)}")

# ============================================================================
# ÉTAPE 4 : Test de détection d'erreurs
# ============================================================================
st.header("Étape 4 -- Test de détection d'erreurs")
st.markdown(
    "On soumet une ligne contenant une erreur au LLM. "
    "Il doit la détecter en la comparant avec la BDD embeddée (similarité cosinus entre vecteurs)."
)

if st.button("Lancer les tests de détection"):
    try:
        from sentence_transformers import SentenceTransformer
        from error_detection_step import (
            test_scenario_1_wrong_sector,
            test_scenario_2_mismatched_siren,
            test_scenario_3_typo_company_name,
            test_scenario_4_valid_company,
            detect_errors_in_record,
            load_embeddings,
        )

        # Load reference embeddings from data/ dir or session
        emb_files = sorted(DATA_DIR.glob("entreprises_embeddings_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not emb_files and "embeddings_data" not in st.session_state:
            st.warning("Aucun fichier d'embeddings trouvé. Lance les étapes 1 et 2 d'abord.")
        else:
            if emb_files:
                reference_embeddings = load_embeddings(str(emb_files[0]))
            else:
                reference_embeddings = st.session_state.embeddings_data
            model = SentenceTransformer('all-MiniLM-L6-v2')

            scenarios = [
                ("Scénario 1 -- Secteur incorrect", "L'entreprise Orange est soumise avec le secteur Agriculture au lieu de Télécommunications.", test_scenario_1_wrong_sector()),
                ("Scénario 2 -- SIREN mal appareillé", "BNP Paribas est soumise avec le SIREN d'Orange.", test_scenario_2_mismatched_siren()),
                ("Scénario 3 -- Faute de frappe dans le nom", "Le nom 'Orane' est soumis au lieu de 'Orange'.", test_scenario_3_typo_company_name()),
                ("Scénario 4 -- Données valides", "Total Energies avec des données correctes. Le LLM ne devrait pas détecter d'erreur.", test_scenario_4_valid_company()),
            ]

            for scenario_name, description, test_record in scenarios:
                st.subheader(scenario_name)
                st.caption(description)

                is_valid, errors, similarities = detect_errors_in_record(
                    test_record, reference_embeddings, model, similarity_threshold=0.75
                )

                # Show test record
                st.write(f"**Entreprise testée :** {test_record['name']} (SIREN: {test_record['siren']}, Secteur: {test_record.get('secteur', 'N/A')})")

                if is_valid:
                    st.success("Aucune erreur détectée -- la ligne est cohérente avec la base de référence.")
                else:
                    for err in errors:
                        st.warning(err)

                # Similarity scores
                with st.expander("Scores de similarité avec les entreprises de référence"):
                    sim_df = pd.DataFrame([
                        {"Entreprise de référence": name, "Similarité": f"{score:.2%}"}
                        for name, score in sorted(similarities.items(), key=lambda x: x[1], reverse=True)
                    ])
                    st.dataframe(sim_df, use_container_width=True, hide_index=True)

                st.divider()

            st.success("Les 4 scénarios de test ont été exécutés.")

    except Exception as e:
        st.error(f"Erreur lors de la détection : {str(e)}")

# ============================================================================
# Statut de la session
# ============================================================================
st.divider()
st.subheader("Statut de la session")

status_cols = st.columns(4)
with status_cols[0]:
    st.metric("Données", "OK" if "companies_data" in st.session_state else "En attente")
with status_cols[1]:
    st.metric("Embeddings", "OK" if "embeddings_data" in st.session_state else "En attente")
with status_cols[2]:
    st.metric("Snowflake", "A configurer")
with status_cols[3]:
    st.metric("Tests", "Prêt")
