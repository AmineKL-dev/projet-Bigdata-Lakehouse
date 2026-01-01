FROM eclipse-temurin:11-jre

# Installation des dépendances
RUN apt-get update && apt-get install -y \
    curl \
    python3 \
    python3-pip \
    python3-venv \
    && rm -rf /var/lib/apt/lists/*

# Variables d'environnement
ENV SPARK_VERSION=3.5.3
ENV HADOOP_VERSION=3
ENV DELTA_VERSION=3.2.0
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Environnement virtuel Python
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
ENV PYSPARK_PYTHON=$VIRTUAL_ENV/bin/python

# Téléchargement de Spark depuis Apache Archive
RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -o spark.tgz \
    && tar -xzf spark.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
    && rm spark.tgz

# Téléchargement des JARs Delta Lake
RUN curl -L https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar -o $SPARK_HOME/jars/delta-spark_2.12-${DELTA_VERSION}.jar \
    && curl -L https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar -o $SPARK_HOME/jars/delta-storage-${DELTA_VERSION}.jar

# Installation des dépendances Python
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copie du code
WORKDIR /app
COPY spark/ /app/spark/
COPY sensors/ /app/sensors/

CMD ["python", "spark/ingestion_lakehouse.py"]
