import os
import logging
from pyspark.sql import SparkSession, DataFrame
from schemas import final_schema
from pyspark.sql.functions import from_json, col, row_number

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



# Bronze reader

def read_bronze(spark: SparkSession, jdbc_url: str, jdbc_props: dict) -> DataFrame:
    """Read bronze.raw_fintech_data and parse the JSONB column using partitioned JDBC reads."""
    logger.info("Determining min/max ID bounds for bronze.raw_fintech_data...")

    # Run a quick query to fetch the minimum and maximum IDs for partitioning
    bounds_df = spark.read.jdbc(
        url=jdbc_url,
        table="(SELECT MIN(id) as min_id, MAX(id) as max_id FROM bronze.raw_fintech_data) t",
        properties=jdbc_props
    )
    bounds = bounds_df.collect()[0]
    min_id = bounds["min_id"]
    max_id = bounds["max_id"]

    if min_id is not None and max_id is not None and max_id > min_id:
        # Use a fixed partition count of 4 for local Docker execution
        num_partitions = 4
        logger.info(f"Reading bronze.raw_fintech_data in parallel (partitions={num_partitions}, bounds=[{min_id}, {max_id}])")
        raw_df = spark.read.jdbc(
            url=jdbc_url,
            table="(SELECT id, data::text AS data, load_timestamp FROM bronze.raw_fintech_data) t",
            column="id",
            lowerBound=min_id,
            upperBound=max_id,
            numPartitions=num_partitions,
            properties=jdbc_props
        )
    else:
        logger.warning("Table is empty or bounds are invalid. Falling back to single-threaded read.")
        raw_df = spark.read.jdbc(
            url=jdbc_url,
            table="(SELECT id, data::text AS data, load_timestamp FROM bronze.raw_fintech_data) t",
            properties=jdbc_props
        )

    parsed = raw_df.withColumn("parsed", from_json(col("data").cast("string"), final_schema))
    logger.info("Loaded and parsed raw records from Bronze.")
    return parsed