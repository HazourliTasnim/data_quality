import json
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
from pathlib import Path
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_snowflake_connection():
    """
    Create a Snowflake connection using environment variables
    Required .env variables:
    - SNOWFLAKE_USER
    - SNOWFLAKE_PASSWORD
    - SNOWFLAKE_ACCOUNT
    - SNOWFLAKE_WAREHOUSE
    - SNOWFLAKE_DATABASE
    - SNOWFLAKE_SCHEMA
    """
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("thazourli_snwoflake_user"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
        )
        return conn
    except Exception as e:
        print(f"Erreur de connexion Snowflake: {e}")
        return None

def create_embedding_table(conn):
    """Create table for storing embeddings in Snowflake"""
    cursor = conn.cursor()
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS COMPANY_EMBEDDINGS (
        COMPANY_ID INT AUTOINCREMENT PRIMARY KEY,
        COMPANY_NAME VARCHAR,
        SIREN VARCHAR,
        SIRET VARCHAR,
        SECTEUR VARCHAR,
        FORME_JURIDIQUE VARCHAR,
        EMPLOYEES VARCHAR,
        EMBEDDING_DIMENSION INT,
        EMBEDDING_MODEL VARCHAR,
        EMBEDDING VECTOR(FLOAT, 384),
        CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );
    """
    
    try:
        cursor.execute(create_table_sql)
        print("Table COMPANY_EMBEDDINGS créée/vérifiée")
        return True
    except Exception as e:
        print(f"Erreur création table: {e}")
        return False
    finally:
        cursor.close()

def load_embeddings_json(json_file):
    """Load embeddings from JSON file"""
    with open(json_file, 'r', encoding='utf-8') as f:
        return json.load(f)

def insert_embeddings_to_snowflake(conn, embeddings_data):
    """Insert embeddings into Snowflake"""
    cursor = conn.cursor()
    
    insert_sql = """
    INSERT INTO COMPANY_EMBEDDINGS (
        COMPANY_NAME, SIREN, SIRET, SECTEUR, FORME_JURIDIQUE, EMPLOYEES,
        EMBEDDING_DIMENSION, EMBEDDING_MODEL, EMBEDDING
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    try:
        for company in embeddings_data:
            embedding_vector = company.get('embedding', [])
            
            cursor.execute(insert_sql, (
                company.get('name'),
                company.get('siren'),
                company.get('siret'),
                company.get('secteur'),
                company.get('forme_juridique'),
                company.get('employees'),
                company.get('embedding_dimension'),
                company.get('embedding_model'),
                embedding_vector
            ))
        
        conn.commit()
        print(f"{len(embeddings_data)} enregistrements insérés dans Snowflake")
        return True
    except Exception as e:
        print(f"Erreur insertion Snowflake: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()

def demo_mode():
    """Show what would happen in demo mode (without actual Snowflake connection)"""
    print("MODE DÉMONSTRATION - Étape 3 Snowflake\n")
    
    # Find latest embeddings file
    json_files = list(Path(".").glob("entreprises_embeddings_*.json"))
    if not json_files:
        print("Aucun fichier entreprises_embeddings_*.json trouvé!")
        return False
    
    latest_json = max(json_files, key=lambda p: p.stat().st_mtime)
    print(f"Lecture embeddings depuis: {latest_json}\n")
    
    embeddings_data = load_embeddings_json(latest_json)
    
    print("Structure des données pour Snowflake:")
    print("""
    CREATE TABLE COMPANY_EMBEDDINGS (
        COMPANY_ID INT PRIMARY KEY,
        COMPANY_NAME VARCHAR,
        SIREN VARCHAR,
        SIRET VARCHAR,
        SECTEUR VARCHAR,
        FORME_JURIDIQUE VARCHAR,
        EMPLOYEES VARCHAR,
        EMBEDDING_DIMENSION INT,
        EMBEDDING_MODEL VARCHAR,
        EMBEDDING VECTOR(FLOAT, 384),
        CREATED_AT TIMESTAMP_NTZ,
        UPDATED_AT TIMESTAMP_NTZ
    );
    """)
    
    print(f"\nDonnées à charger ({len(embeddings_data)} entreprises):")
    for company in embeddings_data:
        print(f"  • {company['name']} ({company['siren']})")
        print(f"    - Embedding: {len(company['embedding'])} dimensions")
        print(f"    - Modèle: {company['embedding_model']}")
    
    return True

if __name__ == "__main__":
    print("Étape 3 - Stockage dans Snowflake...\n")
    
    # Check for Snowflake credentials
    if os.getenv("SNOWFLAKE_USER"):
        print("Credentials Snowflake trouvés\n")
        
        # Connect to Snowflake
        conn = get_snowflake_connection()
        if not conn:
            print("Impossible de se connecter à Snowflake")
            print("Passage en mode démonstration...\n")
            demo_mode()
        else:
            # Create table
            if create_embedding_table(conn):
                # Find latest embeddings file
                json_files = list(Path(".").glob("entreprises_embeddings_*.json"))
                if json_files:
                    latest_json = max(json_files, key=lambda p: p.stat().st_mtime)
                    print(f"Lecture embeddings depuis: {latest_json}\n")
                    
                    # Load and insert
                    embeddings_data = load_embeddings_json(latest_json)
                    insert_embeddings_to_snowflake(conn, embeddings_data)
                    
                    print("\nÉtape 3 - Terminée!")
                    print("Les embeddings sont maintenant dans Snowflake")
                    print("Prêt pour l'étape 4 (Test de détection d'erreurs)")
                else:
                    print("Aucun fichier embeddings trouvé")
            
            conn.close()
    else:
        print("Aucuns credentials Snowflake trouvés dans .env")
        print("Pour connecter à Snowflake, ajoute à .env:")
        print("""
SNOWFLAKE_USER=ton_utilisateur
SNOWFLAKE_PASSWORD=ton_mot_de_passe
SNOWFLAKE_ACCOUNT=ton_account_id
SNOWFLAKE_WAREHOUSE=TON_WAREHOUSE
SNOWFLAKE_DATABASE=TA_DATABASE
SNOWFLAKE_SCHEMA=TON_SCHEMA
        """)
        print("\n" + "="*60)
        demo_mode()
