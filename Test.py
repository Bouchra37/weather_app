import requests
import json
import datetime

# Ta clé API
api_key = "3740f591db5a3cd5050c407b25551cca"

# URL de l'API d'OpenWeatherMap
url = "https://api.openweathermap.org/data/2.5/weather"

# Liste des villes à interroger
cities = ["Paris", "El Jadida"]

# Parcourir la liste des villes
for city in cities:
    params = {
        'q': city,        # Ville
        'appid': api_key, # Clé API
        'units': 'metric',
        'lang': 'fr'
    }

    # Envoi de la requête
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        # Récupérer les données JSON
        data = response.json()
        
        # Conversion du timestamp en temps lisible
        timestamp = data['dt']
        date_time = datetime.datetime.fromtimestamp(timestamp)
        
        print(f"--- Données pour {city} ---")
        print("Date et heure des données :", date_time.strftime("%Y-%m-%d %H:%M:%S"))
        print(json.dumps(data, indent=4, ensure_ascii=False))
    else:
        print(f"Erreur pour {city}: {response.status_code}, {response.text}")
