import sys
import logging
from pyspark.sql.functions import col, explode_outer, to_json
from spark_utils import get_spark_session, get_jdbc_config, read_bronze, dedup, write_silver

# Configure standard logging to stream stdout
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("spark_staging")


def main():
    logger.info("Initializing Spark Session for Silver staging run...")
    spark = get_spark_session("run_spark_silver")
    spark.sparkContext.setLogLevel("WARN")
    jdbc_url, jdbc_props = get_jdbc_config()

    # 1. Read & parse Bronze once
    parsed = read_bronze(spark, jdbc_url, jdbc_props)
    parsed.cache()
    logger.info("Cached parsed Bronze DataFrame.")

    # 2. Build and write stg_customers
    logger.info("Building stg_customers...")
    stg_customers_raw = parsed.select(
        col("id"),
        col("parsed.customer_id"),
        col("parsed.first_name"),
        col("parsed.last_name"),
        col("parsed.email"),
        col("parsed.phone_number"),
        col("parsed.address"),
        col("parsed.city"),
        col("parsed.country"),
        col("parsed.lat"),
        col("parsed.lon"),
        col("parsed.gender"),
        col("parsed.date_of_birth"),
        col("parsed.nationality"),
        col("parsed.registration_date"),
        col("parsed.status"),
        col("parsed.kyc_status"),
        col("parsed.risk_score"),
        col("parsed.customer_segment"),
        col("parsed.relationship_manager"),
        # Storing nested objects as JSON strings for downstream dbt flattening
        to_json(col("parsed.credit_info")).alias("credit_info"),
        to_json(col("parsed.digital_engagement")).alias("digital_engagement"),
    )
    stg_customers = dedup(stg_customers_raw, partition_by=["customer_id"]).drop("id")
    write_silver(stg_customers, "stg_customers", jdbc_url, jdbc_props)

    # 3. Build and write stg_accounts
    logger.info("Building stg_accounts...")
    stg_accounts_raw = parsed.select(
        col("id"),
        col("parsed.customer_id"),
        explode_outer(col("parsed.accounts")).alias("account"),
    ).select(
        col("id"),
        col("customer_id"),
        col("account.account_id"),
        col("account.account_type"),
        col("account.currency"),
        col("account.balance"),
        col("account.credit_limit"),
        col("account.interest_rate"),
        col("account.opened_date"),
        col("account.status"),
        col("account.branch_code"),
    ).filter(
        col("account_id").isNotNull()
    )
    stg_accounts = dedup(stg_accounts_raw, partition_by=["account_id"]).drop("id")
    write_silver(stg_accounts, "stg_accounts", jdbc_url, jdbc_props)

    # 4. Build and write stg_loans
    logger.info("Building stg_loans...")
    stg_loans_raw = parsed.select(
        col("id"),
        col("parsed.customer_id"),
        explode_outer(col("parsed.loans")).alias("loan"),
    ).select(
        col("id"),
        col("customer_id"),
        col("loan.loan_id"),
        col("loan.type"),
        col("loan.principal"),
        col("loan.outstanding_balance"),
        col("loan.interest_rate"),
        col("loan.term_months"),
        col("loan.monthly_payment"),
        col("loan.start_date"),
        col("loan.end_date"),
        col("loan.status"),
        col("loan.days_past_due"),
        col("loan.collateral_type"),
    ).filter(
        col("loan_id").isNotNull()
    )
    stg_loans = dedup(stg_loans_raw, partition_by=["loan_id"]).drop("id")
    write_silver(stg_loans, "stg_loans", jdbc_url, jdbc_props)

    # 5. Build and write stg_transactions
    logger.info("Building stg_transactions...")
    stg_transactions_raw = parsed.select(
        col("id"),
        col("parsed.customer_id"),
        explode_outer(col("parsed.transactions")).alias("txn"),
    ).select(
        col("id"),
        col("customer_id"),
        col("txn.transaction_id"),
        col("txn.account_id"),
        col("txn.date"),
        col("txn.amount"),
        col("txn.currency"),
        col("txn.type"),
        col("txn.category"),
        col("txn.merchant"),
        col("txn.channel"),
        col("txn.status"),
        col("txn.description"),
    ).filter(
        col("transaction_id").isNotNull()
    )
    stg_transactions = dedup(stg_transactions_raw, partition_by=["transaction_id"]).drop("id")
    write_silver(stg_transactions, "stg_transactions", jdbc_url, jdbc_props)

    # Clean up
    parsed.unpersist()
    spark.stop()
    logger.info("All Silver staging tables written successfully.")


if __name__ == "__main__":
    main()
