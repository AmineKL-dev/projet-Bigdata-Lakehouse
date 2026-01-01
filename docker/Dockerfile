FROM eclipse-temurin:11-jre

# Installation des dépendances
RUN apt-get update && apt-get install -y \
    curl \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Variables d'environnement
ENV SPARK_VERSION=3.5.3
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_PYTHON=python3

# Téléchargement de Spark depuis Apache Archive
RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -o spark.tgz \
    && tar -xzf spark.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
    && rm spark.tgz

# Installation des dépendances Python
COPY requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir -r /app/requirements.txt

# Copie du code
WORKDIR /app
COPY spark/ /app/spark/
COPY sensors/ /app/sensors/

CMD ["python3", "spark/ingestion_lakehouse.py"]
