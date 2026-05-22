import os
import logging
from pyspark.sql import SparkSession, DataFrame
from schemas import final_schema

# Get logger for Spark staging utils
logger = logging.getLogger("spark_staging.utils")


# ── Spark + JDBC config ───────────────────────────────────────────────────────

def get_spark_session(app_name: str) -> SparkSession:
    """Create a local-mode SparkSession with the PostgreSQL JDBC driver."""
    jar = os.getenv("SPARK_JDBC_JAR", "/opt/airflow/jars/postgresql-42.7.3.jar")
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars", jar)
        .config("spark.driver.extraClassPath", jar)
        .config("spark.executor.extraClassPath", jar)
        .getOrCreate()
    )


def get_jdbc_config() -> tuple:
    """Return (jdbc_url, jdbc_properties) from environment variables."""
    host     = os.getenv("POSTGRES_HOST",     "postgres")
    port     = os.getenv("POSTGRES_PORT",     "5432")
    db       = os.getenv("POSTGRES_DB",       "qversity")
    user     = os.getenv("POSTGRES_USER",     "qversity-admin")
    password = os.getenv("POSTGRES_PASSWORD", "qversity-admin")

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"
    jdbc_props = {
        "user":      user,
        "password":  password,
        "driver":    "org.postgresql.Driver",
        "fetchsize": "10000",
    }
    return jdbc_url, jdbc_props