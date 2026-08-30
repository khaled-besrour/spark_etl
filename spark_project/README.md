# Spark - projet de formation

## Reseau partage avec Airflow

A creer une seule fois avant de lancer l'un ou l'autre projet :

```bash
docker network create formation_network
```

## Lancer le cluster Spark

```bash
cd spark_project
docker compose up -d
```

- UI master : http://localhost:8081
- UI workers : http://localhost:8082 / 8083 / 8084

3 workers (`spark-worker-1/2/3`), 1 core / 1G RAM chacun (voir `docker-compose.yaml`).

## Exemple word count

```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-jobs/example_word_count.py /opt/spark-data/sample.txt
```

Ou en local (sans docker) :

```bash
pip install -r requirements.txt
python jobs/example_word_count.py data/sample.txt
```

## Arreter le cluster

```bash
docker compose down
```

## Pipeline ELT : Retail Sales Data (Kaggle)

Dataset : [berkayalan/retail-sales-data](https://www.kaggle.com/datasets/berkayalan/retail-sales-data).
Trois CSV en etoile : `sales.csv` (faits), `store_cities.csv` (magasin -> ville),
`product_hierarchy.csv` (non utilise).

Le DAG `retail_sales_elt_dag` (voir `airflow_project/README.md`) lance tout le pipeline.
Les etapes ci-dessous servent a lancer chaque job a la main, pour du debug.

### 1. Telecharger le dataset

Compte Kaggle + jeton API requis (Kaggle -> Settings -> "Create New API Token"), a mettre
dans `KAGGLE_USERNAME` / `KAGGLE_KEY` (ou `~/.kaggle/kaggle.json`).

```bash
pip install -r requirements.txt
python -m ingestion.kaggle_download --dest data/raw/retail_sales
```

Idempotent (skip si des CSV existent deja dans `--dest`, `--force` pour retelecharger).

### 2. Demarrer Postgres

```bash
docker compose up -d postgres-raw
```

- `jdbc:postgresql://postgres-raw:5432/raw_data` (depuis le reseau `formation_network`) ou
  `localhost:5433` depuis l'hote, user/password `raw`/`raw`.
- Schemas crees au premier demarrage (`sql/init_raw_db.sql`). Les tables sont creees par
  Spark au premier chargement.

### 3. Configurer `.env`

```bash
copy .env.example .env      # Windows
# cp .env.example .env      # Linux/Mac
```

Optionnel (des defauts sont codes dans `settings.py`), mais recommande.

### 4. ELT : sales.csv + store_cities.csv -> raw.retail_sales

```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-jobs/retail_sales_elt.py \
  --input /opt/spark-data/raw/retail_sales
```

Joint les deux CSV sur `store_id`, derive `year`/`month`/`week`/`day_name` depuis `date`.
Aucun filtrage ici, c'est un chargement brut.

### 5. Stats sur la table raw

```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-jobs/retail_sales_stats.py
```

Calcule nombre de lignes, lignes completes, lignes "pertinentes", nulls par colonne.
Ecrit une nouvelle ligne dans `raw.retail_sales_stats` a chaque run.

### 6. Staging : cle + filtrage qualite

```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-jobs/retail_sales_staging.py
```

Calcule `global_object_key` (hash de `product_id, store_id, date, sales, revenue`), rejette
les lignes avec une colonne pertinente manquante ou `stock`/`sales`/`revenue` a 0.
Ecrit dans `staging.retail_sales` (valides) et `staging.retail_sales_rejected` (avec
`reject_reason`).

### 7. Intermediate : agregats business

```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-jobs/retail_sales_intermediate.py
```

Quatre vues, recalculees a chaque run :

- `intermediate.monthly_sales_by_product`
- `intermediate.daily_sales_by_product`
- `intermediate.monthly_sales_by_store`
- `intermediate.sales_by_day_name_by_product` (par annee + jour de semaine)

### 8. Training : features pour un modele de prevision

```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-jobs/retail_sales_training_table.py
```

Ecrit `training.product_month_features` : une ligne par produit/mois, features
`num_stores`/`month`/`total_stock`/`avg_price`, cible `total_sales`.

### 9. Export Parquet + Snowflake

Les jobs sous `jobs/` ne font que de l'injection Postgres. `export/` s'occupe du reste :

```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/export/parquet_exporter.py
```

Relit `intermediate.*` et `training.product_month_features`, ecrit chacune en Parquet sous
`PARQUET_EXPORT_DIR/<schema>/<table>/` (`/opt/spark-data/exports` par defaut).

```bash
pip install -r requirements.txt
python -m export.snowflake_loader --export-dir data/exports
```

Charge les Parquet dans Snowflake (stage utilisateur `@~`, `PUT` + `COPY INTO`). Cree la
table si absente (schema infere), la vide avant chaque chargement. Necessite un compte
Snowflake (`SNOWFLAKE_*` dans `.env`) ; optionnel, ignore si `SNOWFLAKE_ACCOUNT` est vide.

## Qualite de code (avant de push)

```bash
pip install -r requirements-dev.txt

black jobs ingestion export tests settings.py
isort jobs ingestion export tests settings.py
ruff check jobs ingestion export tests settings.py
pytest tests -v
```

La CI (`ci-spark.yml`) rejoue ces etapes sur chaque push/PR touchant `spark_project/`.
