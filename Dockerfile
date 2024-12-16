# Utilise une image Python avec Debian plus complète
FROM python:3.9-bullseye

# Installer Java nécessaire pour Spark
RUN apt-get update && apt-get install -y openjdk-11-jdk

# Définir le répertoire de travail
WORKDIR /app

# Configurer JAVA_HOME
ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Copier le code du producteur, consommateur, et le script Spark Streaming dans le conteneur
COPY producer.py . 
COPY consumer.py . 
COPY spark_streaming.py . 

# Installer les dépendances nécessaires
RUN pip install kafka-python requests pyspark cassandra-driver pandas pyarrow

# Copier le script pour démarrer les services
COPY start.sh .

# Donner les droits d'exécution au script
RUN chmod +x start.sh

# Commande à exécuter lors du démarrage du conteneur
CMD ["./start.sh"]
