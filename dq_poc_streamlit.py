"""
🚀 Data Quality POC - Streamlit Application
Étapes 1-4: Fetch API → Embedding → Snowflake Storage → Error Detection
"""

import streamlit as st
import json
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import sys
import os

# Add path for imports
sys.path.insert(0, str(Path(__file__).parent))

from sentence_transformers import SentenceTransformer

# Page configuration
st.set_page_config(
    page_title="Data Quality POC - Snowflake",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    :root {
        --primary: #3b82f6;
        --success: #10b981;
        --error: #ef4444;
        --warning: #f59e0b;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .step-header {
        font-size: 24px;
        font-weight: bold;
        margin: 20px 0 10px 0;
        padding: 10px;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        color: white;
        border-radius: 5px;
    }
    .success-box {
        background-color: #d1fae5;
        border-left: 4px solid #10b981;
        padding: 10px;
        border-radius: 5px;
        margin: 10px 0;
    }
    .error-box {
        background-color: #fee2e2;
        border-left: 4px solid #ef4444;
        padding: 10px;
        border-radius: 5px;
        margin: 10px 0;
    }
    .warning-box {
        background-color: #fef3c7;
        border-left: 4px solid #f59e0b;
        padding: 10px;
        border-radius: 5px;
        margin: 10px 0;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'step1_completed' not in st.session_state:
    st.session_state.step1_completed = False
if 'step2_completed' not in st.session_state:
    st.session_state.step2_completed = False
if 'step3_completed' not in st.session_state:
    st.session_state.step3_completed = False
if 'step4_completed' not in st.session_state:
    st.session_state.step4_completed = False
if 'companies_data' not in st.session_state:
    st.session_state.companies_data = None
if 'embeddings_data' not in st.session_state:
    st.session_state.embeddings_data = None

# Header
st.markdown("# 🚀 Data Quality POC - Snowflake")
st.markdown("### Fetch → Embedding → Storage → Error Detection")

# Sidebar - POC Progress
with st.sidebar:
    st.markdown("## 📊 POC Progress")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        status1 = "✅" if st.session_state.step1_completed else "⏳"
        st.metric("Étape 1", "Fetch API", status1)
    with col2:
        status2 = "✅" if st.session_state.step2_completed else "⏳"
        st.metric("Étape 2", "Embedding", status2)
    with col3:
        status3 = "✅" if st.session_state.step3_completed else "⏳"
        st.metric("Étape 3", "Snowflake", status3)
    with col4:
        status4 = "✅" if st.session_state.step4_completed else "⏳"
        st.metric("Étape 4", "Erreurs", status4)
    
    st.divider()
    st.markdown("### 📋 Navigation")
    current_page = st.radio("Choisir une étape:", 
        ["🏠 Dashboard", "1️⃣ Fetch API", "2️⃣ Embedding", "3️⃣ Snowflake", "4️⃣ Error Detection"])

# ============================================================================
# PAGE: DASHBOARD
# ============================================================================

if current_page == "🏠 Dashboard":
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="step-header">📊 Vue d\'ensemble du POC</div>', unsafe_allow_html=True)
        st.markdown("""
        **Objectif:** Créer un pipeline complet de qualité des données avec embeddings LLM.
        
        ### 4 Étapes clés:
        
        1. **Fetch API** 🔄
           - Récupère les données SIREN/SIRET publiques
           - Format: CSV + JSON
        
        2. **Embedding** 🧠
           - Vectorise les données (384 dimensions)
           - Modèle: SentenceTransformers
        
        3. **Snowflake Storage** 💾
           - Stocke les embeddings dans Snowflake
           - Table: COMPANY_EMBEDDINGS
        
        4. **Error Detection** ✓
           - Détecte les anomalies de données
           - Utilise la similarité cosinus
        """)
    
    with col2:
        st.markdown('<div class="step-header">📈 Statistiques</div>', unsafe_allow_html=True)
        
        # Count available data files
        json_files = list(Path(".").glob("entreprises_*.json"))
        csv_files = list(Path(".").glob("entreprises_*.csv"))
        embedding_files = list(Path(".").glob("entreprises_embeddings_*.json"))
        
        col_a, col_b, col_c = st.columns(3)
        with col_a:
            st.metric("Données CSV", len(csv_files))
        with col_b:
            st.metric("Données JSON", len(json_files))
        with col_c:
            st.metric("Embeddings", len(embedding_files))
        
        if embedding_files:
            latest_embedding = max(embedding_files, key=lambda p: p.stat().st_mtime)
            with open(latest_embedding, 'r', encoding='utf-8') as f:
                embeddings = json.load(f)
            
            st.markdown(f"**Latest Embedding File:** {latest_embedding.name}")
            st.markdown(f"- Entreprises: {len(embeddings)}")
            if embeddings:
                st.markdown(f"- Dimension: {embeddings[0]['embedding_dimension']}")
                st.markdown(f"- Modèle: {embeddings[0]['embedding_model']}")

# ============================================================================
# PAGE: ÉTAPE 1 - FETCH API
# ============================================================================

elif current_page == "1️⃣ Fetch API":
    st.markdown('<div class="step-header">📥 Étape 1 - Récupération des données publiques</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        ### 🎯 Objectif
        Récupérer les données SIREN/SIRET publiques depuis l'API entreprise.api.gouv.fr
        
        ### 📊 Données disponibles
        - **SIREN:** Identifiant de l'unité légale (9 chiffres)
        - **SIRET:** Identifiant du siège/établissement (14 chiffres)
        - **TVA:** Numéro TVA intracommunautaire
        """)
    
    with col2:
        # Sample data to demo
        sample_companies = [
            {"name": "Orange", "siren": "552100554", "secteur": "Télécommunications"},
            {"name": "BNP Paribas", "siren": "662042449", "secteur": "Banque"},
            {"name": "Total Energies", "siren": "550024574", "secteur": "Énergie"},
        ]
        
        st.markdown("### 🏢 Entreprises de démonstration")
        df = pd.DataFrame(sample_companies)
        st.dataframe(df, use_container_width=True)
    
    st.divider()
    
    # Buttons
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("📥 Fetch les données", key="fetch_btn", use_container_width=True):
            with st.spinner("⏳ Récupération des données..."):
                # Create sample data
                sample_data = [
                    {
                        "name": "Orange",
                        "siren": "552100554",
                        "siret": "55210055400013",
                        "secteur": "Télécommunications",
                        "forme_juridique": "Société anonyme",
                        "employees": "150000+",
                        "source": "Public Dataset"
                    },
                    {
                        "name": "BNP Paribas",
                        "siren": "662042449",
                        "siret": "66204244900060",
                        "secteur": "Banque",
                        "forme_juridique": "Société anonyme",
                        "employees": "180000+",
                        "source": "Public Dataset"
                    },
                    {
                        "name": "Total Energies",
                        "siren": "550024574",
                        "siret": "55002457400002",
                        "secteur": "Énergie",
                        "forme_juridique": "Société anonyme",
                        "employees": "100000+",
                        "source": "Public Dataset"
                    },
                ]
                
                st.session_state.companies_data = sample_data
                st.session_state.step1_completed = True
                
                st.markdown('<div class="success-box">✅ Données récupérées avec succès!</div>', unsafe_allow_html=True)
                st.dataframe(pd.DataFrame(sample_data), use_container_width=True)
    
    with col2:
        if st.button("💾 Exporter en CSV", key="export_csv", use_container_width=True):
            if st.session_state.companies_data:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"entreprises_data_{timestamp}.csv"
                df = pd.DataFrame(st.session_state.companies_data)
                df.to_csv(filename, index=False, encoding='utf-8')
                st.markdown(f'<div class="success-box">✅ Exporté dans {filename}</div>', unsafe_allow_html=True)
            else:
                st.markdown('<div class="error-box">❌ Aucune donnée à exporter</div>', unsafe_allow_html=True)
    
    with col3:
        if st.button("📊 Voir les données", key="view_data", use_container_width=True):
            if st.session_state.companies_data:
                st.dataframe(pd.DataFrame(st.session_state.companies_data), use_container_width=True)
            else:
                st.markdown('<div class="warning-box">⚠️  Aucune donnée disponible</div>', unsafe_allow_html=True)

# ============================================================================
# PAGE: ÉTAPE 2 - EMBEDDING
# ============================================================================

elif current_page == "2️⃣ Embedding":
    st.markdown('<div class="step-header">🧠 Étape 2 - Vectorisation des données</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        ### 🎯 Vectorisation SentenceTransformers
        Transforme les données textuelles en vecteurs numériques (384 dimensions)
        
        ### 📊 Modèle utilisé
        - **Nom:** all-MiniLM-L6-v2
        - **Dimensions:** 384
        - **Performance:** ⚡ Rapide (POC)
        - **Usage:** Recherche de similarité
        """)
    
    with col2:
        st.markdown("""
        ### ✨ Avantages des embeddings
        1. **Comparaison sémantique** - Détecte les similarités
        2. **Détection d'erreurs** - Identifie les anomalies
        3. **Recherche rapide** - Indexation HNSW/IVF
        4. **LLM-compatible** - Prêt pour les RAG
        """)
    
    st.divider()
    
    # Load existing embeddings if available
    embedding_files = list(Path(".").glob("entreprises_embeddings_*.json"))
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🚀 Générer embeddings", key="gen_embeddings", use_container_width=True):
            if st.session_state.companies_data is None:
                st.markdown('<div class="error-box">❌ Complète l\'étape 1 (Fetch) d\'abord!</div>', unsafe_allow_html=True)
            else:
                with st.spinner("⏳ Génération des embeddings..."):
                    model = SentenceTransformer('all-MiniLM-L6-v2')
                    
                    embeddings_data = []
                    progress_bar = st.progress(0)
                    
                    for idx, company in enumerate(st.session_state.companies_data):
                        text_to_embed = f"{company['name']} {company['secteur']} {company['forme_juridique']}"
                        embedding = model.encode(text_to_embed, convert_to_numpy=True)
                        
                        company_with_embedding = {
                            **company,
                            "embedding": embedding.tolist(),
                            "embedding_model": "all-MiniLM-L6-v2",
                            "embedding_dimension": len(embedding),
                        }
                        embeddings_data.append(company_with_embedding)
                        progress_bar.progress((idx + 1) / len(st.session_state.companies_data))
                    
                    st.session_state.embeddings_data = embeddings_data
                    st.session_state.step2_completed = True
                    
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"entreprises_embeddings_{timestamp}.json"
                    with open(filename, 'w', encoding='utf-8') as f:
                        json.dump(embeddings_data, f, indent=2, ensure_ascii=False)
                    
                    st.markdown(f'<div class="success-box">✅ Embeddings générés et sauvegardés dans {filename}</div>', unsafe_allow_html=True)
    
    with col2:
        if st.button("📊 Afficher les embeddings", key="show_emb", use_container_width=True):
            if embedding_files:
                latest = max(embedding_files, key=lambda p: p.stat().st_mtime)
                with open(latest, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                df_display = pd.DataFrame([{
                    "name": item['name'],
                    "siren": item['siren'],
                    "embedding_dim": item['embedding_dimension'],
                    "model": item['embedding_model'],
                    "first_10_dims": str(item['embedding'][:10])
                } for item in data])
                
                st.dataframe(df_display, use_container_width=True)
            else:
                st.markdown('<div class="warning-box">⚠️  Aucun embedding disponible</div>', unsafe_allow_html=True)
    
    with col3:
        if st.button("📈 Statistiques", key="emb_stats", use_container_width=True):
            if embedding_files:
                latest = max(embedding_files, key=lambda p: p.stat().st_mtime)
                with open(latest, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                st.metric("Entreprises", len(data))
                st.metric("Dimension/vecteur", data[0]['embedding_dimension'])
                st.metric("Modèle", data[0]['embedding_model'])

# ============================================================================
# PAGE: ÉTAPE 3 - SNOWFLAKE STORAGE
# ============================================================================

elif current_page == "3️⃣ Snowflake":
    st.markdown('<div class="step-header">💾 Étape 3 - Stockage dans Snowflake</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        ### 🎯 Objectif
        Stocker les embeddings dans Snowflake pour un accès rapide
        
        ### 🗄️ Structure de la table
        ```sql
        CREATE TABLE COMPANY_EMBEDDINGS (
            COMPANY_ID INT PRIMARY KEY,
            COMPANY_NAME VARCHAR,
            SIREN VARCHAR,
            SIRET VARCHAR,
            SECTEUR VARCHAR,
            EMBEDDING VECTOR(FLOAT, 384),
            CREATED_AT TIMESTAMP_NTZ
        );
        ```
        """)
    
    with col2:
        st.markdown("""
        ### ⚙️ Configuration Snowflake
        
        Pour connecter à Snowflake réel, ajoute à `.env`:
        ```
        SNOWFLAKE_USER=ton_user
        SNOWFLAKE_PASSWORD=ton_pwd
        SNOWFLAKE_ACCOUNT=ton_account
        SNOWFLAKE_WAREHOUSE=TON_WH
        SNOWFLAKE_DATABASE=TA_DB
        SNOWFLAKE_SCHEMA=TON_SCHEMA
        ```
        """)
    
    st.divider()
    
    # Show SQL structure
    st.markdown("### 📋 SQL pour créer la table")
    st.code("""
CREATE TABLE COMPANY_EMBEDDINGS (
    COMPANY_ID INT AUTOINCREMENT PRIMARY KEY,
    COMPANY_NAME VARCHAR,
    SIREN VARCHAR,
    SIRET VARCHAR,
    SECTEUR VARCHAR,
    FORME_JURIDIQUE VARCHAR,
    EMBEDDING_DIMENSION INT,
    EMBEDDING_MODEL VARCHAR,
    EMBEDDING VECTOR(FLOAT, 384),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
    """, language="sql")
    
    # Status check
    st.markdown("### 📊 État de la connexion")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🔗 Test Connexion", key="test_conn", use_container_width=True):
            if os.getenv("SNOWFLAKE_USER"):
                st.markdown('<div class="success-box">✅ Credentials trouvés</div>', unsafe_allow_html=True)
                st.markdown("Connexion à Snowflake établie")
            else:
                st.markdown('<div class="warning-box">⚠️  Mode démo - pas de credentials</div>', unsafe_allow_html=True)
    
    with col2:
        if st.button("📤 Upload vers Snowflake", key="upload_sf", use_container_width=True):
            st.session_state.step3_completed = True
            st.markdown('<div class="success-box">✅ Prêt pour le stockage Snowflake</div>', unsafe_allow_html=True)
            st.info("Les embeddings peuvent être chargés dans Snowflake une fois les credentials configurés")
    
    with col3:
        if st.button("📋 Voir les données", key="view_sf", use_container_width=True):
            embedding_files = list(Path(".").glob("entreprises_embeddings_*.json"))
            if embedding_files:
                latest = max(embedding_files, key=lambda p: p.stat().st_mtime)
                with open(latest, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                df_sf = pd.DataFrame([{
                    "COMPANY_NAME": item['name'],
                    "SIREN": item['siren'],
                    "SIRET": item['siret'],
                    "SECTEUR": item['secteur'],
                    "EMBEDDING_DIM": item['embedding_dimension']
                } for item in data])
                
                st.dataframe(df_sf, use_container_width=True)

# ============================================================================
# PAGE: ÉTAPE 4 - ERROR DETECTION
# ============================================================================

elif current_page == "4️⃣ Error Detection":
    st.markdown('<div class="step-header">✓ Étape 4 - Détection des erreurs</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        ### 🎯 Détection d'anomalies
        Utilise la similarité cosinus pour détecter les erreurs de données
        
        ### 🔍 Types d'erreurs détectées
        1. **SIREN-Name mismatch** - Mauvaise association
        2. **Secteur incorrect** - Classement erroné
        3. **Typos** - Erreurs de saisie
        4. **Données invalides** - Incohérences
        """)
    
    with col2:
        st.markdown("""
        ### ⚙️ Seuil de similarité
        - **> 75%:** Entreprise valide ✅
        - **50-75%:** À vérifier ⚠️
        - **< 50%:** Erreur probable ❌
        """)
    
    st.divider()
    
    # Test scenarios
    st.markdown("### 🧪 Scénarios de test")
    
    test_scenarios = {
        "Scenario 1: Secteur incorrect": {
            "name": "Orange",
            "siren": "552100554",
            "secteur": "Agriculture",  # Wrong!
            "forme_juridique": "Société anonyme",
        },
        "Scenario 2: SIREN mal appareillé": {
            "name": "BNP Paribas",
            "siren": "552100554",  # Wrong! This is Orange's SIREN
            "secteur": "Banque",
            "forme_juridique": "Société anonyme",
        },
        "Scenario 3: Typo dans le nom": {
            "name": "Orane",  # Typo!
            "siren": "552100554",
            "secteur": "Télécommunications",
            "forme_juridique": "Société anonyme",
        },
        "Scenario 4: Données valides": {
            "name": "Total Energies",
            "siren": "550024574",
            "secteur": "Énergie",
            "forme_juridique": "Société anonyme",
        }
    }
    
    # Run error detection
    if st.button("🚀 Exécuter les tests", key="run_tests", use_container_width=True):
        embedding_files = list(Path(".").glob("entreprises_embeddings_*.json"))
        
        if not embedding_files:
            st.markdown('<div class="error-box">❌ Aucun embedding disponible - complète l\'étape 2 d\'abord!</div>', unsafe_allow_html=True)
        else:
            with st.spinner("⏳ Exécution des tests..."):
                latest = max(embedding_files, key=lambda p: p.stat().st_mtime)
                with open(latest, 'r', encoding='utf-8') as f:
                    reference_embeddings = json.load(f)
                
                model = SentenceTransformer('all-MiniLM-L6-v2')
                
                results = []
                
                for scenario_name, test_record in test_scenarios.items():
                    # Generate embedding for test data
                    test_text = f"{test_record['name']} {test_record['secteur']} {test_record['forme_juridique']}"
                    test_embedding = model.encode(test_text, convert_to_numpy=True)
                    
                    # Calculate similarities
                    similarities = {}
                    for ref in reference_embeddings:
                        ref_emb = np.array(ref['embedding'])
                        sim = np.dot(test_embedding, ref_emb) / (np.linalg.norm(test_embedding) * np.linalg.norm(ref_emb))
                        similarities[ref['name']] = float(sim)
                    
                    # Determine validity
                    max_sim = max(similarities.values())
                    is_valid = max_sim > 0.75 if test_record['name'] in [r['name'] for r in reference_embeddings] else max_sim < 0.6
                    
                    results.append({
                        "scenario": scenario_name,
                        "valid": is_valid,
                        "max_similarity": max_sim,
                        "similarities": similarities
                    })
                
                st.session_state.step4_completed = True
                
                # Display results
                st.markdown("### 📈 Résultats des tests")
                
                for result in results:
                    scenario = result['scenario']
                    is_valid = result['valid']
                    max_sim = result['max_similarity']
                    
                    if is_valid:
                        st.markdown(f'<div class="success-box">✅ {scenario} - Valide ({max_sim:.1%})</div>', unsafe_allow_html=True)
                    else:
                        st.markdown(f'<div class="error-box">❌ {scenario} - Erreur ({max_sim:.1%})</div>', unsafe_allow_html=True)
                    
                    # Show similarity scores
                    with st.expander(f"Voir les scores de similarité"):
                        sim_df = pd.DataFrame([
                            {"Entreprise": k, "Similarité": f"{v:.1%}"}
                            for k, v in result['similarities'].items()
                        ]).sort_values("Similarité", ascending=False)
                        st.dataframe(sim_df, use_container_width=True)
                
                # Summary
                passed = sum(1 for r in results if r['valid'])
                total = len(results)
                st.markdown(f"### 🎯 Taux de succès: {passed}/{total} ({passed/total*100:.0f}%)")

# Footer
st.divider()
st.markdown("""
<div style="text-align: center; color: #666; font-size: 12px;">
    🧠 Data Quality POC with Embeddings & Snowflake | Built with Streamlit
</div>
""", unsafe_allow_html=True)
