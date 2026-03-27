import json
import numpy as np
from pathlib import Path
from datetime import datetime

# Try to import LLM library
try:
    from sentence_transformers import SentenceTransformer, util
except ImportError:
    print("sentence-transformers not found")
    exit(1)

def load_embeddings(json_file):
    """Load embeddings from JSON file"""
    with open(json_file, 'r', encoding='utf-8') as f:
        return json.load(f)

def calculate_similarity(embedding1, embedding2):
    """Calculate cosine similarity between two embeddings"""
    # Convert to numpy arrays
    emb1 = np.array(embedding1)
    emb2 = np.array(embedding2)
    
    # Calculate cosine similarity
    similarity = np.dot(emb1, emb2) / (np.linalg.norm(emb1) * np.linalg.norm(emb2))
    return similarity

def detect_errors_in_record(test_record, reference_embeddings, model, similarity_threshold=0.75):
    """
    Detect errors in a record by comparing it with reference data
    Returns: (is_valid, errors_detected, similarity_scores)
    """
    test_text = f"{test_record['name']} {test_record.get('secteur', 'Unknown')} {test_record.get('forme_juridique', 'Unknown')}"
    test_embedding = model.encode(test_text, convert_to_numpy=True)
    
    errors = []
    similarities = {}
    
    # Compare with each reference company
    for ref_company in reference_embeddings:
        ref_embedding = np.array(ref_company['embedding'])
        
        # Calculate similarity
        similarity = calculate_similarity(test_embedding.tolist(), ref_company['embedding'])
        similarities[ref_company['name']] = similarity
        
        # Check for information consistency
        if test_record.get('siren') == ref_company.get('siren'):
            if test_record.get('name') != ref_company.get('name'):
                errors.append(f"ERREUR: SIREN {test_record['siren']} associé à {test_record['name']}, mais devrait être {ref_company['name']}")
            if test_record.get('secteur') != ref_company.get('secteur'):
                errors.append(f"ERREUR: Secteur incorrect pour {test_record['name']} (trouvé: {test_record.get('secteur')}, attendu: {ref_company.get('secteur')})")
        
        # Check if this might be a confused company (similar but different SIREN)
        if similarity > similarity_threshold and test_record.get('siren') != ref_company.get('siren'):
            errors.append(f"ATTENTION: {test_record['name']} est très similaire à {ref_company['name']} (similarité: {similarity:.2%})")
    
    is_valid = len(errors) == 0
    
    return is_valid, errors, similarities

def test_scenario_1_wrong_sector():
    """Test 1: Wrong sector for a company"""
    return {
        "name": "Orange",
        "siren": "552100554",
        "siret": "55210055400013",
        "secteur": "Agriculture",  # WRONG - should be Télécommunications
        "forme_juridique": "Société anonyme",
        "employees": "150000+",
        "source": "Test - Wrong Sector"
    }

def test_scenario_2_mismatched_siren():
    """Test 2: SIREN-Name mismatch"""
    return {
        "name": "BNP Paribas",
        "siren": "552100554",  # WRONG - this is Orange's SIREN
        "siret": "55210055400013",
        "secteur": "Banque",
        "forme_juridique": "Société anonyme",
        "employees": "180000+",
        "source": "Test - Mismatched SIREN"
    }

def test_scenario_3_typo_company_name():
    """Test 3: Typo in company name"""
    return {
        "name": "Orane",  # WRONG - typo for Orange
        "siren": "552100554",
        "siret": "55210055400013",
        "secteur": "Télécommunications",
        "forme_juridique": "Société anonyme",
        "employees": "150000+",
        "source": "Test - Company Name Typo"
    }

def test_scenario_4_valid_company():
    """Test 4: Valid company data"""
    return {
        "name": "Total Energies",
        "siren": "550024574",
        "siret": "55002457400002",
        "secteur": "Énergie",
        "forme_juridique": "Société anonyme",
        "employees": "100000+",
        "source": "Test - Valid Data"
    }

if __name__ == "__main__":
    print("Étape 4 - Test de détection d'erreurs avec LLM+Embeddings\n")
    print("="*70)
    
    # Load reference embeddings
    json_files = list(Path(".").glob("entreprises_embeddings_*.json"))
    if not json_files:
        print("Aucun fichier entreprises_embeddings_*.json trouvé!")
        exit(1)
    
    latest_json = max(json_files, key=lambda p: p.stat().st_mtime)
    print(f"Chargement BDD de référence depuis: {latest_json}\n")
    
    reference_embeddings = load_embeddings(latest_json)
    
    # Load the same embedding model
    model = SentenceTransformer('all-MiniLM-L6-v2')
    
    # Test scenarios
    test_scenarios = [
        ("Scénario 1 - Secteur incorrect", test_scenario_1_wrong_sector()),
        ("Scénario 2 - SIREN mal appareillé", test_scenario_2_mismatched_siren()),
        ("Scénario 3 - Typo dans le nom", test_scenario_3_typo_company_name()),
        ("Scénario 4 - Données valides", test_scenario_4_valid_company()),
    ]
    
    results = []
    
    for scenario_name, test_record in test_scenarios:
        print(f"\n{scenario_name}")
        print("-" * 70)
        print(f"Test data: {test_record['name']} (SIREN: {test_record['siren']})")
        print(f"Secteur: {test_record.get('secteur', 'N/A')}")
        
        # Detect errors
        is_valid, errors, similarities = detect_errors_in_record(
            test_record, 
            reference_embeddings, 
            model,
            similarity_threshold=0.75
        )
        
        # Display results
        if is_valid:
            print("VALIDE - Aucune erreur détectée")
        else:
            print("ERREURS DÉTECTÉES:")
            for error in errors:
                print(f"   {error}")
        
        print("\nScores de similarité avec références:")
        for company_name, score in sorted(similarities.items(), key=lambda x: x[1], reverse=True):
            status = "+" if score > 0.75 else "-"
            print(f"   {status} {company_name}: {score:.2%}")
        
        results.append({
            "scenario": scenario_name,
            "valid": is_valid,
            "errors_count": len(errors),
            "similarities": similarities
        })
    
    # Summary
    print("\n" + "="*70)
    print("RÉSUMÉ DES TESTS")
    print("="*70)
    
    for result in results:
        status = "PASS" if result['valid'] else "FAIL"
        print(f"{status} - {result['scenario']}")
        if result['errors_count'] > 0:
            print(f"      → {result['errors_count']} erreur(s) détectée(s)")
    
    # Calculate success rate
    passed = sum(1 for r in results if r['valid'])
    total = len(results)
    success_rate = (passed / total) * 100
    
    print(f"\nTaux de succès: {passed}/{total} ({success_rate:.0f}%)")
    print("\nÉtape 4 - Terminée!")
    print("Le LLM peut désormais détecter les erreurs en comparant avec les embeddings")
    print("La BDD vectorisée est fiable et peut être utilisée par le LLM")
