import pandas as pd

# Chemin vers le fichier Parquet
parquet_file_path = "./output/parquet/weather_data.parquet"

try:
    # Lecture du fichier Parquet avec Pandas
    df = pd.read_parquet(parquet_file_path, engine='pyarrow')  # Ou engine='fastparquet'
    
    # Afficher les 5 premières lignes
    # print("Les 5 premières lignes du fichier Parquet :")
    print(df)
    print(df.tail(50))
    
    # Afficher les colonnes et le nombre de lignes
    print("\nRésumé des colonnes et de la taille du fichier :")
    print(df.info())
except Exception as e:
    print(f"Erreur lors de la lecture du fichier Parquet : {e}")
