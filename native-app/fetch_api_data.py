import requests
import json
import csv
from datetime import datetime

# API Endpoints
SIRENE_SEARCH_API = "https://api.insee.fr/api-de-sirene/3/full_text/search"

def fetch_sirene_data(query, number=3):
    """Fetch real SIRENE data from INSEE public API (NO AUTH REQUIRED)"""
    try:
        params = {
            "q": query,
            "number": number
        }
        response = requests.get(SIRENE_SEARCH_API, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        results = []
        if "resultats" in data:
            for item in data["resultats"]:
                company = item.get("uniteLegale", item)
                results.append({
                    "name": company.get("nomCommercial") or company.get("nomUniteLegale", "N/A"),
                    "siren": company.get("siren", "N/A"),
                    "siret": item.get("siret", "N/A") if "siret" in item else company.get("siren", "N/A") + "00001",
                    "secteur": company.get("activitePrincipaleLibelle", "N/A"),
                    "forme_juridique": company.get("formeJuridiqueLibelle", "N/A"),
                    "employees": company.get("trancheEffectifsEtablissement", "N/A"),
                    "source": "INSEE API (Real Data)"
                })
        
        return results
    except Exception as e:
        print(f"Erreur API: {str(e)}")
        return []

def create_sample_data():
    """Create sample data for demo purposes (fallback)"""
    return [
        {
            "name": "Orange",
            "siren": "552100554",
            "siret": "55210055400013",
            "secteur": "Télécommunications",
            "forme_juridique": "Société anonyme",
            "employees": "150000+",
            "source": "Public Dataset (Demo)"
        },
        {
            "name": "BNP Paribas",
            "siren": "662042449",
            "siret": "66204244900060",
            "secteur": "Banque",
            "forme_juridique": "Société anonyme",
            "employees": "180000+",
            "source": "Public Dataset (Demo)"
        },
        {
            "name": "Total Energies",
            "siren": "550024574",
            "siret": "55002457400127",
            "secteur": "Pétrole et gaz",
            "forme_juridique": "Société anonyme",
            "employees": "100000+",
            "source": "Public Dataset (Demo)"
        },
    ]

def fetch_companies_data(queries=None, use_api=True):
    """Fetch companies data from API or use fallback demo data"""
    if queries is None:
        queries = ["orange", "bnp", "total"]
    
    if use_api:
        print("Fetching from INSEE API (REAL DATA)...\n")
        all_results = []
        for query in queries:
            print(f"Searching: {query}")
            results = fetch_sirene_data(query, number=1)
            if results:
                all_results.extend(results)
                print(f"   Found: {results[0]['name']}")
            else:
                print(f"   No results, using demo data")
        
        if all_results:
            return all_results
    
    print("Using demo data (fallback)")
    return create_sample_data()

def save_to_csv(data_list, filename):
    """Save fetched data to CSV file"""
    if not data_list:
        return
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=data_list[0].keys())
        writer.writeheader()
        writer.writerows(data_list)
    print(f"Données sauvegardées dans {filename}")

def save_to_json(data_list, filename):
    """Save fetched data to JSON file"""
    if not data_list:
        return
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data_list, f, indent=2, ensure_ascii=False)
    print(f"Données sauvegardées dans {filename}")

if __name__ == "__main__":
    print("Data Fetching - Étape 1 du POC\n")
    
    # Try to fetch from API, fallback to demo data
    sample_data = fetch_companies_data(use_api=True)
    
    print("\nEntreprises fetched:\n")
    for company in sample_data:
        print(f"{company['name']}")
        print(f"   SIREN: {company['siren']}")
        print(f"   SIRET: {company['siret']}")
        print(f"   Secteur: {company['secteur']}")
        print(f"   Source: {company['source']}")
        print()
    
    # Save to CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"entreprises_data_{timestamp}.csv"
    save_to_csv(sample_data, csv_filename)
    
    # Save to JSON
    json_filename = f"entreprises_data_{timestamp}.json"
    save_to_json(sample_data, json_filename)
    
    print("Summary:")
    print(json.dumps(sample_data, indent=2, ensure_ascii=False))
