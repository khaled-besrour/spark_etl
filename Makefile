NETWORK := formation_network

.PHONY: network up down clean build db clean-data fc fmt

network:
	docker network inspect $(NETWORK) >/dev/null 2>&1 || docker network create $(NETWORK)

build:
	cd spark_project && docker compose build
	cd airflow_project && docker compose build

up:
	cd spark_project && docker compose up -d
	cd airflow_project && docker compose up -d
	@echo "Spark UI : http://localhost:8081 | Airflow UI : http://localhost:8080 (admin/admin)"

down:
	cd airflow_project && docker compose down
	cd spark_project && docker compose down

clean:
	cd airflow_project && docker compose down -v
	cd spark_project && docker compose down -v

db:
	cd spark_project && docker compose exec postgres-raw psql -U raw -d raw_data


# Vide les tables de donnees sans supprimer les volumes/schemas (contrairement
# a `clean`). Le nettoyage des exports Parquet passe par spark-master (pas
# `rm` sur l'hote, pas fiable depuis `make` sous Windows).
clean-data:
	cd spark_project && docker compose exec spark-master rm -rf /opt/spark-data/exports
	cd spark_project && docker compose exec -T postgres-raw psql -U raw -d raw_data -c "TRUNCATE TABLE raw.retail_sales, raw.retail_sales_stats, staging.retail_sales, staging.retail_sales_rejected, intermediate.monthly_sales_by_product, intermediate.daily_sales_by_product, intermediate.monthly_sales_by_store, intermediate.sales_by_day_name_by_product, training.product_month_features;"

# Comme clean-data, mais garde raw : pour rejouer staging/intermediate/
# training/export sans re-telecharger et recharger le CSV.
fc:
	cd spark_project && docker compose exec spark-master rm -rf /opt/spark-data/exports
	cd spark_project && docker compose exec -T postgres-raw psql -U raw -d raw_data -c "TRUNCATE TABLE staging.retail_sales, staging.retail_sales_rejected, intermediate.monthly_sales_by_product, intermediate.daily_sales_by_product, intermediate.monthly_sales_by_store, intermediate.sales_by_day_name_by_product, training.product_month_features;"

# Corrige automatiquement ce que la CI verifie (formatage, imports, lint).
# Necessite : pip install -r requirements-dev.txt (dans .venv a la racine).
# Appelle directement l'interpreteur du venv plutot que de dependre d'une
# activation : chaque ligne de recette `make` tourne dans son propre
# sous-shell jetable, donc "activer" un venv dans une regle n'a aucun effet
# sur les lignes suivantes ni sur votre shell interactif.
# Pas de `cd ..` : sous cmd.exe (SHELL par defaut de `make` sous Windows), un
# `/` non protege dans le nom de la commande est lu comme un switch, pas
# comme un separateur de chemin -- "../.venv/..." est alors compris comme la
# commande "..", d'ou "'..' n'est pas reconnu...". Chemin entre guillemets,
# racine du repo, sans "..".
PY := ".venv/Scripts/python.exe"

fmt:
	$(PY) -m black spark_project/jobs spark_project/ingestion spark_project/export spark_project/tests spark_project/settings.py
	$(PY) -m isort spark_project/jobs spark_project/ingestion spark_project/export spark_project/tests spark_project/settings.py
	$(PY) -m ruff check --fix spark_project/jobs spark_project/ingestion spark_project/export spark_project/tests spark_project/settings.py
	$(PY) -m black airflow_project/dags airflow_project/tests
	$(PY) -m isort airflow_project/dags airflow_project/tests
	$(PY) -m ruff check --fix airflow_project/dags airflow_project/tests
