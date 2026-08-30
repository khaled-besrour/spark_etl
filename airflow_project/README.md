# Airflow - projet de formation

## Reseau partage avec Spark

```bash
docker network create formation_network
```

Lancer le cluster Spark (`spark_project/README.md`) **avant** Airflow.

## Lancer Airflow en local

Airflow 3 : le webserver s'appelle `api-server`, et `dag-processor` est obligatoire
(role auparavant tenu par le scheduler). `triggerer` tourne aussi par defaut.

```bash
cd airflow_project
copy .env.example .env      # Windows
# cp .env.example .env      # Linux/Mac

docker compose build
docker compose up airflow-init   # cree la base + l'utilisateur admin
docker compose up -d
```

Puis http://localhost:8080 (`admin` / `admin`, voir `.env`).

> Le login stable `admin`/`admin` vient du provider `apache-airflow-providers-fab`
> (`AIRFLOW__CORE__AUTH_MANAGER=FabAuthManager`, voir docker-compose.yaml). Par defaut
> Airflow 3 genere un mot de passe aleatoire a chaque demarrage.

Les DAGs sont en pause par defaut, a activer via l'UI ou :

```bash
docker compose exec airflow-scheduler airflow dags unpause hello_world_dag
```

`hello_world_dag` : message -> `date` (bash) -> message -> soumission de
`example_word_count.py` sur le cluster Spark.

## DAG `retail_sales_elt_dag`

Pipeline complet : Kaggle -> raw -> staging -> agregats + table d'entrainement -> Parquet ->
Snowflake (optionnel). Detail des taches dans le docstring de `dags/retail_sales_elt_dag.py`
et dans `spark_project/README.md`.

Prealables :

- `spark_project` demarre avec `postgres-raw` (`docker compose up -d`).
- `spark_project/.env` cree (copie de `.env.example`) : le job Spark lit la connexion
  Postgres de la, pas d'ici. `SNOWFLAKE_*` optionnel.
- `KAGGLE_USERNAME` / `KAGGLE_KEY` dans `.env` (voir https://www.kaggle.com/docs/api).
- Unpause : `docker compose exec airflow-scheduler airflow dags unpause retail_sales_elt_dag`.

### Erreur "Could not parse Master URL: 'spark-master:7077'"

Bug connu du hook Spark ([apache/airflow#46169](https://github.com/apache/airflow/issues/46169)).
Ce repo contourne le probleme en definissant `AIRFLOW_CONN_SPARK_DEFAULT` en JSON avec
`host` = `spark://spark-master` (voir docker-compose.yaml). Si vous recreez cette connexion
vous-meme, gardez le prefixe `spark://` dans le champ Host.

### Erreur "Connection refused" lors de la soumission Spark

- Verifier le reseau : `docker network inspect formation_network`.
- Verifier que `spark-master` est resolvable depuis Airflow :
  `docker compose exec airflow-scheduler ping -c1 spark-master`.
- Le driver tourne dans ce conteneur (deploy-mode client) et doit s'annoncer correctement
  aux executors, sinon `Connection refused` cote executor. Deux confs necessaires :
  - `spark.driver.host=airflow-scheduler`
  - `spark.driver.bindAddress=0.0.0.0`

  `dags/spark_helpers.py` les fixe par defaut via `build_spark_submit_task`.

## Arreter / nettoyer

```bash
docker compose down          # arrete
docker compose down -v       # arrete + supprime le volume postgres
```

## Qualite de code (avant de push)

```bash
pip install -r requirements-dev.txt

black dags tests
isort dags tests
ruff check dags tests
pytest tests -v
```

La CI (`ci-airflow.yml`) rejoue ces etapes sur chaque push/PR touchant `airflow_project/`.
