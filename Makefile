NETWORK := formation_network

.PHONY: network up down clean build db clean-data fc

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
