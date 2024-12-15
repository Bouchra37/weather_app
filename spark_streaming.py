from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, IntegerType
from pyspark.sql.functions import col, from_json, to_timestamp, avg, explode
import logging
logger = logging.getLogger('spark')
logger.setLevel(logging.INFO)

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 pyspark-shell'
# KAFKA_BROKER = "kafka:9092"
# TOPIC_NAME = "weather-data"

# # Configuration Spark
# spark = SparkSession.builder \
#     .appName("Spark Kafka Stream") \
#     .master("spark://spark-master:7077") \
#     .getOrCreate()

# # Connexion avec Kafka en streaming
# kafka_stream = spark.readStream.format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BROKER) \
#     .option("subscribe", TOPIC_NAME) \
#     .load()

# # Schéma attendu pour les données
# weather_schema = StructType([
#     StructField("coord", StructType([
#         StructField("lon", DoubleType()),
#         StructField("lat", DoubleType())
#     ])),
#     StructField("weather", ArrayType(StructType([
#         StructField("id", IntegerType()),
#         StructField("main", StringType()),
#         StructField("description", StringType()),
#         StructField("icon", StringType())
#     ]))),
#     StructField("base", StringType()),
#     StructField("main", StructType([
#         StructField("temp", DoubleType()),
#         StructField("feels_like", DoubleType()),
#         StructField("temp_min", DoubleType()),
#         StructField("temp_max", DoubleType()),
#         StructField("pressure", DoubleType()),
#         StructField("humidity", DoubleType()),
#         StructField("sea_level", DoubleType()),
#         StructField("grnd_level", DoubleType())
#     ])),
#     StructField("visibility", IntegerType()),
#     StructField("wind", StructType([
#         StructField("speed", DoubleType()),
#         StructField("deg", IntegerType())
#     ])),
#     StructField("clouds", StructType([
#         StructField("all", IntegerType())
#     ])),
#     StructField("dt", IntegerType()),
#     StructField("sys", StructType([
#         StructField("type", IntegerType()),
#         StructField("id", IntegerType()),
#         StructField("country", StringType()),
#         StructField("sunrise", IntegerType()),
#         StructField("sunset", IntegerType())
#     ])),
#     StructField("timezone", IntegerType()),
#     StructField("id", IntegerType()),
#     StructField("name", StringType()),
#     StructField("cod", IntegerType())
# ])

# # Décodage des messages JSON
# transformed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as json_string") \
#     .withColumn("json_data", from_json(col("json_string"), weather_schema)) \
#     .select(
#         col("json_data.name").alias("city"),
#         col("json_data.main.temp").alias("temperature"),
#         col("json_data.main.humidity").alias("humidity"),
#         col("json_data.dt").alias("timestamp")  # Utilisation du timestamp Unix
#     )


# # Création de la vue temporaire
# transformed_stream.createOrReplaceTempView("weather_data_temp_view")

# # Requêtes Spark SQL pour l'analyse
# # 1. Requête agrégée : Moyenne des températures et de l'humidité
# logger.info("Requête pour la moyenne des températures et de l'humidité par ville :")
# aggregated_query = spark.sql("""
#     SELECT city, 
#            AVG(temperature) AS avg_temperature, 
#            AVG(humidity) AS avg_humidity 
#     FROM weather_data_temp_view 
#     GROUP BY city
# """)
# aggregated_query.writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .option("truncate", "false")  \
#     .option("numRows", 40) \
#     .start()

# # 2. Requête : Filtrage des villes avec température > 20°C
# logger.info("\nRequête pour les villes avec température > 20°C :")
# high_temp_query = spark.sql("""
#     SELECT city, temperature as temperature_gt_20, humidity
#     FROM weather_data_temp_view
#     WHERE temperature > 20
# """)
# high_temp_query.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false")  \
#     .option("numRows", 40) \
#     .start()

# # 3. Requête : Filtrage des villes avec humidité > 50%
# logger.info("\nRequête pour les villes avec humidité > 50% :")
# high_humidity_query = spark.sql("""
#     SELECT city, temperature, humidity as humidity_gt_50
#     FROM weather_data_temp_view
#     WHERE humidity > 50
# """)
# high_humidity_query.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .option("numRows", 40) \
#     .start()

# # 4. Requête : Analyse des conditions extrêmes (temp > 20 et humidité > 50)
# logger.info("\nRequête pour les conditions extrêmes (temp > 20 et humidité > 50) :")
# extreme_conditions_query = spark.sql("""
#     SELECT city, temperature as temperature_gt_20, humidity as humidity_gt_50
#     FROM weather_data_temp_view
#     WHERE temperature > 20 AND humidity > 50
# """)
# extreme_conditions_query.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .option("numRows", 40) \
#     .start()

# # Attente de l'arrêt de tous les flux
# spark.streams.awaitAnyTermination()


KAFKA_BROKER = "kafka:9092"
TOPIC_NAME = "weather-data"

# Configuration Spark
spark = SparkSession.builder \
    .appName("Spark Kafka Stream") \
    .master("spark://spark-master:7077") \
    .config("spark.cassandra.connection.host", "cassandra")  \
    .config("spark.cassandra.connection.port", "9042")  \
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
        to_timestamp(col("json_data.timestamp")).alias("timestamp")  # Conversion en timestamp
    )

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



def calculate_avg_temp(df, batch_id):
    avg_temp_df = df.groupBy("city").agg(avg("temperature").alias("avg_temperature"))
    avg_temp_df.createOrReplaceTempView("temp_avg_temperature")
    avg_temp_df.show()

query_avg_temp = transformed_stream.writeStream \
    .outputMode("append") \
    .foreachBatch(calculate_avg_temp) \
    .start()

# Requête 2 : Villes avec humidité supérieure à un seuil
def filter_high_humidity(df, batch_id):
    high_humidity_df = df.filter(col("humidity") > 50) \
                         .select("city", "humidity", col("humidity").alias("humidity_gt_50"))
    high_humidity_df.createOrReplaceTempView("temp_high_humidity")
    high_humidity_df.show()

query_high_humidity = transformed_stream.writeStream \
    .outputMode("append") \
    .foreachBatch(filter_high_humidity) \
    .start()

# Requête 3 : Comptage des descriptions météo par type
def count_weather_descriptions(df, batch_id):
    weather_count_df = df.select(explode(col("description")).alias("desc")) \
                         .groupBy("desc") \
                         .count()
    weather_count_df.createOrReplaceTempView("temp_weather_description_count")
    weather_count_df.show()

query_weather_count = transformed_stream.writeStream \
    .outputMode("append") \
    .foreachBatch(count_weather_descriptions) \
    .start()
    


 
query_cassandra.awaitTermination()
query_temp.awaitTermination()
query_avg_temp.awaitTermination()
query_high_humidity.awaitTermination()
query_weather_count.awaitTermination()