# Image Airflow custom : ajoute Java (requis par spark-submit) et le
# provider Spark + pyspark pour pouvoir soumettre des jobs vers le
# cluster Spark depuis les DAGs (SparkSubmitOperator).
#
# Java 17 (et non 21) : l'image apache/airflow est basee sur Debian bookworm,
# dont les depots APT ne fournissent pas openjdk-21 (disponible seulement a
# partir de Debian trixie/sid). Ce n'est pas un probleme : Spark 4.x exige
# uniquement Java >= 17 sur chaque process, le driver (ici, cote Airflow)
# et les executors (cote cluster Spark, en Java 21) peuvent tourner sur des
# versions differentes sans souci de compatibilite.
#
# Python 3.10 (et non 3.11) : CONTRAIREMENT a Java, PySpark exige que driver
# et executors tournent sur la MEME version mineure de Python (sinon
# PYTHON_VERSION_MISMATCH a l'execution). Le cluster Spark (image
# apache/spark:4.1.3-...-python3-ubuntu, voir spark_project/Dockerfile)
# embarque Python 3.10 ; comme le driver tourne ici en deploy-mode client
# (cf. SparkSubmitOperator), cette image doit matcher exactement.
FROM apache/airflow:3.3.1-python3.10

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
