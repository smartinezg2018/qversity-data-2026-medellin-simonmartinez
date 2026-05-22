from pyspark.sql.types import (
    ArrayType, StructType, StructField, StringType
)

loans_schema = ArrayType(StructType([
    StructField("loan_id",             StringType(), True),
    StructField("type",                StringType(), True),
    StructField("principal",           StringType(), True),
    StructField("outstanding_balance", StringType(), True),
    StructField("interest_rate",       StringType(), True),
    StructField("term_months",         StringType(), True),
    StructField("monthly_payment",     StringType(), True),
    StructField("start_date",          StringType(), True),
    StructField("end_date",            StringType(), True),
    StructField("status",              StringType(), True),
    StructField("days_past_due",       StringType(), True),
    StructField("collateral_type",     StringType(), True),
]))

accounts_schema = ArrayType(StructType([
    StructField("account_id",    StringType(), True),
    StructField("account_type",  StringType(), True),
    StructField("currency",      StringType(), True),
    StructField("balance",       StringType(), True),
    StructField("credit_limit",  StringType(), True),
    StructField("interest_rate", StringType(), True),
    StructField("opened_date",   StringType(), True),
    StructField("status",        StringType(), True),
    StructField("branch_code",   StringType(), True),
]))

transactions_schema = ArrayType(StructType([
    StructField("transaction_id", StringType(), True),
    StructField("account_id",     StringType(), True),
    StructField("date",           StringType(), True),
    StructField("amount",         StringType(), True),
    StructField("currency",       StringType(), True),
    StructField("type",           StringType(), True),
    StructField("category",       StringType(), True),
    StructField("merchant",       StringType(), True),
    StructField("channel",        StringType(), True),
    StructField("status",         StringType(), True),
    StructField("description",    StringType(), True),
]))

credit_info_schema = StructType([
    StructField("credit_score",              StringType(), True),
    StructField("utilization_pct",           StringType(), True),
    StructField("total_limit",               StringType(), True),
    StructField("total_used",                StringType(), True),
    StructField("num_credit_accounts",       StringType(), True),
    StructField("oldest_account_age_months", StringType(), True),
    StructField("late_payments_12m",         StringType(), True),
    StructField("inquiries_6m",              StringType(), True),
    StructField("bankruptcy_flag",           StringType(), True),
])

digital_engagement_schema = StructType([
    StructField("mobile_app_registered",  StringType(), True),
    StructField("web_banking_registered", StringType(), True),
    StructField("last_login_date",        StringType(), True),
    StructField("avg_monthly_logins",     StringType(), True),
    StructField("preferred_channel",      StringType(), True),
    StructField("push_notifications",     StringType(), True),
    StructField("paperless_statements",   StringType(), True),
])

final_schema = StructType([
    StructField("lat",                  StringType(),              True),
    StructField("lon",                  StringType(),              True),
    StructField("city",                 StringType(),              True),
    StructField("email",                StringType(),              True),
    StructField("loans",                loans_schema,              True),
    StructField("gender",               StringType(),              True),
    StructField("status",               StringType(),              True),
    StructField("address",              StringType(),              True),
    StructField("country",              StringType(),              True),
    StructField("accounts",             accounts_schema,           True),
    StructField("last_name",            StringType(),              True),
    StructField("first_name",           StringType(),              True),
    StructField("kyc_status",           StringType(),              True),
    StructField("risk_score",           StringType(),              True),
    StructField("customer_id",          StringType(),              True),
    StructField("nationality",          StringType(),              True),
    StructField("phone_number",         StringType(),              True),
    StructField("transactions",         transactions_schema,       True),
    StructField("date_of_birth",        StringType(),              True),
    StructField("customer_segment",     StringType(),              True),
    StructField("registration_date",    StringType(),              True),
    StructField("relationship_manager", StringType(),              True),
    StructField("credit_info",          credit_info_schema,        True),
    StructField("digital_engagement",   digital_engagement_schema, True),
])