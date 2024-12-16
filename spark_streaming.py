from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, IntegerType
from pyspark.sql.functions import col, from_json, to_timestamp, avg, explode
import logging
logger = logging.getLogger('spark')
logger.setLevel(logging.INFO)

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 pyspark-shell'

KAFKA_BROKER = "kafka:9092" 
TOPIC_NAME = "weather-data"

# Configuration Spark
spark = SparkSession.builder \
    .appName("Spark Kafka Stream") \
    .master("spark://spark-master:7077") \
    .config("spark.cassandra.connection.host", "cassandra")  \
    .config("spark.cassandra.connection.port", "9042")  \
    .config("spark.local.dir", "/data/spark-temp") \
    .getOrCreate()

# Connexion avec Kafka en streaming
kafka_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC_NAME) \
    .load()

# Schéma attendu pour les données
weather_schema = StructType([
    StructField("coord", StructType([
        StructField("lon", DoubleType()),
        StructField("lat", DoubleType())
    ])),
    StructField("weather", ArrayType(StructType([
        StructField("id", IntegerType()),
        StructField("main", StringType()),
        StructField("description", StringType()),
        StructField("icon", StringType())
    ]))),
    StructField("base", StringType()),
    StructField("main", StructType([
        StructField("temp", DoubleType()),
        StructField("feels_like", DoubleType()),
        StructField("temp_min", DoubleType()),
        StructField("temp_max", DoubleType()),
        StructField("pressure", DoubleType()),
        StructField("humidity", DoubleType()),
        StructField("sea_level", DoubleType()),
        StructField("grnd_level", DoubleType())
    ])),
    StructField("visibility", IntegerType()),
    StructField("wind", StructType([
        StructField("speed", DoubleType()),
        StructField("deg", IntegerType())
    ])),
    StructField("clouds", StructType([
        StructField("all", IntegerType())
    ])),
    StructField("dt", IntegerType()),
    StructField("sys", StructType([
        StructField("type", IntegerType()),
        StructField("id", IntegerType()),
        StructField("country", StringType()),
        StructField("sunrise", IntegerType()),
        StructField("sunset", IntegerType())
    ])),
    StructField("timezone", IntegerType()),
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("cod", IntegerType()),
    StructField("timestamp", StringType())
])

# Décodage des messages JSON
transformed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as json_string") \
    .withColumn("json_data", from_json(col("json_string"), weather_schema)) \
    .select(
        col("json_data.name").alias("city"),
        col("json_data.main.temp").alias("temperature"),
        col("json_data.main.humidity").alias("humidity"),
        col("json_data.weather.description").alias("description"),
        to_timestamp(col("json_data.timestamp")).alias("timestamp")  
    )

import os
import pandas as pd
def write_to_parquet(spark_df, batch_id):
    # output_dir = "/data/parquet"
    # output_dir.mkdir(parents=True, exist_ok=True)
    output_path = "/data/parquet/weather_data.parquet"  # Nom du fichier Parquet constant
    try:
        # Convertir le DataFrame Spark en Pandas DataFrame
        pandas_df = spark_df.toPandas()

        # Si le fichier Parquet existe déjà
        if os.path.exists(output_path):
            # Lire les données existantes
            existing_data = pd.read_parquet(output_path)

            # Concaténer les nouvelles données avec les anciennes
            combined_data = pd.concat([existing_data, pandas_df], ignore_index=True)
        else:
            # Si le fichier n'existe pas, utiliser uniquement les nouvelles données
            combined_data = pandas_df

        # Écrire les données combinées dans le fichier Parquet
        combined_data.to_parquet(output_path, index=False, engine='pyarrow', compression='snappy')

        print(f"Batch {batch_id} ajouté avec succès dans {output_path}")
    except Exception as e:
        print(f"Erreur lors de l'écriture du batch {batch_id} dans Parquet : {e}")

# Configuration du stream vers Parquet
query_parquet = transformed_stream.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_parquet) \
    .start()



# Fonction pour écrire dans Cassandra
def write_to_cassandra(df, batch_id):
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="weather_data_history", keyspace="weather") \
        .mode("append") \
        .save()

# Fonction pour écrire dans une table temporaire Spark
def write_to_temp_table(df, batch_id):
    # Écriture dans la table temporaire Spark
    df.createOrReplaceTempView("temp_weather_data")  # Création ou mise à jour de la vue temporaire
    df.show()
# Envoi dans Cassandra avec foreachBatch
query_cassandra = transformed_stream.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_cassandra) \
    .start()

# Écriture dans la table temporaire avec foreachBatch
query_temp = transformed_stream.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_temp_table) \
    .start()

query_cassandra.awaitTermination()
query_temp.awaitTermination()
query_parquet.awaitTermination()

