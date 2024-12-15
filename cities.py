import requests

# API GeoNames
GEO_NAMES_USERNAME = "bouchraenn37"  # Remplacez par votre nom d'utilisateur GeoNames
GEO_NAMES_URL = "http://api.geonames.org/searchJSON"

def get_cities_in_country(country_code):
    params = {
        'country': country_code,
        'maxRows': 1000,  # Limité à 1000 résultats pour la version gratuite
        'username': GEO_NAMES_USERNAME,
        'featureClass': 'P',  # 'P' pour les villes
    }
    response = requests.get(GEO_NAMES_URL, params=params)
    if response.status_code == 200:
        cities = [geoname['name'] for geoname in response.json()['geonames']]
        return cities
    else:
        print(f"Erreur API GeoNames: {response.status_code}")
        return []

# Exemple d'utilisation
cities = get_cities_in_country("MA")  # "MA" pour le Maroc
print(cities)  # Affiche la liste des villes
