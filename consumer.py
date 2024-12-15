from kafka import KafkaConsumer
import json

# Configuration Kafka
KAFKA_BROKER = "kafka:9092"  # Adresse du broker Kafka
TOPIC_NAME = "weather-data"   # Le nom du topic à consommer

# Initialisation du consommateur Kafka
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    group_id="weather-consumer-group",  # Nom du groupe de consommateurs
    auto_offset_reset="earliest",      # Lire les messages depuis le début
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print(f"Consommation des messages du topic : {TOPIC_NAME}")
print("-" * 40)

# Boucle pour consommer les messages
try:
    for message in consumer:
        print(f"Message reçu : {message.value}")
except KeyboardInterrupt:
    print("Arrêt du consommateur.")
finally:
    consumer.close()
