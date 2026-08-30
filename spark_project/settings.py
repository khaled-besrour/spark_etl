"""Configuration du job retail_sales_elt, chargee depuis un fichier .env.

Le fichier attendu est spark_project/.env (voir .env.example). Ce module et ce
.env sont montes individuellement dans /opt/spark-jobs (a cote du script) par
les conteneurs Spark et Airflow, voir docker-compose.yaml des deux projets.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

_ENV_FILE = Path(__file__).resolve().parent / ".env"
if _ENV_FILE.is_file():
    load_dotenv(_ENV_FILE)


@dataclass(frozen=True)
class Settings:
    raw_db_host: str = os.environ.get("RAW_DB_HOST", "postgres-raw")
    raw_db_port: str = os.environ.get("RAW_DB_PORT", "5432")
    raw_db_name: str = os.environ.get("RAW_DB_NAME", "raw_data")
    raw_db_user: str = os.environ.get("RAW_DB_USER", "raw")
    raw_db_password: str = os.environ.get("RAW_DB_PASSWORD", "raw")
    raw_table: str = os.environ.get("RAW_DB_TABLE", "raw.retail_sales")
    stats_table: str = os.environ.get("RAW_DB_STATS_TABLE", "raw.retail_sales_stats")
    staging_table: str = os.environ.get("STAGING_DB_TABLE", "staging.retail_sales")
    staging_rejected_table: str = os.environ.get(
        "STAGING_DB_REJECTED_TABLE", "staging.retail_sales_rejected"
    )
    intermediate_monthly_by_product_table: str = os.environ.get(
        "INTERMEDIATE_MONTHLY_BY_PRODUCT_TABLE", "intermediate.monthly_sales_by_product"
    )
    intermediate_daily_by_product_table: str = os.environ.get(
        "INTERMEDIATE_DAILY_BY_PRODUCT_TABLE", "intermediate.daily_sales_by_product"
    )
    intermediate_monthly_by_store_table: str = os.environ.get(
        "INTERMEDIATE_MONTHLY_BY_STORE_TABLE", "intermediate.monthly_sales_by_store"
    )
    intermediate_day_name_by_product_table: str = os.environ.get(
        "INTERMEDIATE_DAY_NAME_BY_PRODUCT_TABLE", "intermediate.sales_by_day_name_by_product"
    )
    training_product_month_table: str = os.environ.get(
        "TRAINING_PRODUCT_MONTH_TABLE", "training.product_month_features"
    )
    parquet_export_dir: str = os.environ.get("PARQUET_EXPORT_DIR", "/opt/spark-data/exports")

    # Vide par defaut : necessite un vrai compte Snowflake, optionnel.
    snowflake_account: str = os.environ.get("SNOWFLAKE_ACCOUNT", "")
    snowflake_user: str = os.environ.get("SNOWFLAKE_USER", "")
    snowflake_password: str = os.environ.get("SNOWFLAKE_PASSWORD", "")
    snowflake_warehouse: str = os.environ.get("SNOWFLAKE_WAREHOUSE", "")
    snowflake_database: str = os.environ.get("SNOWFLAKE_DATABASE", "")
    snowflake_role: str = os.environ.get("SNOWFLAKE_ROLE", "")

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.raw_db_host}:{self.raw_db_port}/{self.raw_db_name}"


settings = Settings()
