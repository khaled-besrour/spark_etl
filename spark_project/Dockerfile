# Image Spark custom : ajoute pip + les dependances Python des jobs
# (python-dotenv pour settings.py, kaggle si un job en avait besoin) a
# l'image officielle apache/spark, qui ne contient que PySpark/Java par
# defaut (aucun package tiers).
FROM apache/spark:4.1.3-scala2.13-java21-python3-ubuntu

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3-pip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /requirements.txt
RUN pip3 install --no-cache-dir -r /requirements.txt
