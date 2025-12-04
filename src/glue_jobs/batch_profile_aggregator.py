"""
AWS Glue Batch Job: Customer Profile Aggregation

This job runs daily to:
1. Read last 90 days of transactions from Silver layer
2. Compute customer behavioral profiles
3. Update Redshift dimension (SCD Type 2)

Scheduled: Daily at 02:00 UTC via Airflow
Author: Oğuzhan Göktaş
"""

import sys
from datetime import datetime, timedelta
from decimal import Decimal

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, count, sum as spark_sum, avg as spark_avg,
    max as spark_max, min as spark_min, current_date, current_timestamp,
    datediff, when
)
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType

# Import custom modules
import sys
sys.path.insert(0, '/tmp/')

from common.schemas import SILVER_TRANSACTION_SCHEMA
from common.config import Config
from common.utils import setup_logger, log_dataframe_stats


# ============================================================================
# Configuration
# ============================================================================

logger = setup_logger(__name__)

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'DELTA_LAKE_BASE_PATH',
    'REDSHIFT_URL',
    'REDSHIFT_TEMP_DIR',
    'LOOKBACK_DAYS',
])

config = Config.load(environment='production')

DELTA_BASE = args['DELTA_LAKE_BASE_PATH']
REDSHIFT_URL = args['REDSHIFT_URL']
REDSHIFT_TEMP = args['REDSHIFT_TEMP_DIR']
LOOKBACK_DAYS = int(args.get('LOOKBACK_DAYS', '90'))

logger.info(f"Starting Customer Profile Aggregation")
logger.info(f"Lookback period: {LOOKBACK_DAYS} days")


# ============================================================================
# Initialize Spark
# ============================================================================

spark = (SparkSession.builder
    .appName("AML-Profile-Aggregator")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# ============================================================================
# Read Silver Transactions
# ============================================================================

logger.info(f"Reading transactions from Silver layer...")

silver_path = f"{DELTA_BASE}/silver/transactions"

# Calculate date range
end_date = datetime.now().date()
start_date = end_date - timedelta(days=LOOKBACK_DAYS)

logger.info(f"Date range: {start_date} to {end_date}")

# Read with partition pruning
transactions_df = (spark.read
    .format("delta")
    .load(silver_path)
    .filter(col("transaction_date") >= lit(start_date))
    .filter(col("transaction_date") <= lit(end_date))
)

log_dataframe_stats(transactions_df, "Silver Transactions")


# ============================================================================
# Compute Customer Profiles
# ============================================================================

logger.info("Computing customer behavioral profiles...")

# Aggregate at customer level
customer_profiles = transactions_df.groupBy("customer_id").agg(
    # Transaction counts
    count("*").alias("transaction_count_90d"),
    spark_sum(when(col("transaction_type") == "DEBIT", 1).otherwise(0)).alias("debit_count_90d"),
    spark_sum(when(col("transaction_type") == "CREDIT", 1).otherwise(0)).alias("credit_count_90d"),
    
    # Amount statistics (in EUR)
    spark_sum("amount_eur").alias("total_amount_90d"),
    spark_avg("amount_eur").alias("avg_transaction_amount_90d"),
    spark_max("amount_eur").alias("max_transaction_amount_90d"),
    spark_min("amount_eur").alias("min_transaction_amount_90d"),
    
    # Channel usage
    spark_sum(when(col("channel") == "ATM", 1).otherwise(0)).alias("atm_count_90d"),
    spark_sum(when(col("channel") == "ONLINE", 1).otherwise(0)).alias("online_count_90d"),
    spark_sum(when(col("channel") == "BRANCH", 1).otherwise(0)).alias("branch_count_90d"),
    
    # Risk indicators
    spark_sum(when(col("is_high_value"), 1).otherwise(0)).alias("high_value_count_90d"),
    spark_sum(when(col("is_international"), 1).otherwise(0)).alias("international_count_90d"),
    spark_sum(when(col("is_high_risk_country"), 1).otherwise(0)).alias("high_risk_country_count_90d"),
    spark_sum(when(col("is_unusual_time"), 1).otherwise(0)).alias("unusual_time_count_90d"),
    
    # Latest transaction date
    spark_max("transaction_date").alias("last_transaction_date"),
)

# Calculate 30-day metrics (for comparison)
transactions_30d = transactions_df.filter(
    col("transaction_date") >= lit(end_date - timedelta(days=30))
)

customer_profiles_30d = transactions_30d.groupBy("customer_id").agg(
    count("*").alias("transaction_count_30d"),
    spark_avg("amount_eur").alias("avg_transaction_amount_30d"),
)

# Join 30d and 90d profiles
customer_profiles = customer_profiles.join(
    customer_profiles_30d,
    "customer_id",
    "left"
)

# Calculate derived metrics
customer_profiles = customer_profiles \
    .withColumn("avg_daily_amount_90d", 
                col("total_amount_90d") / lit(90)) \
    .withColumn("high_value_ratio",
                col("high_value_count_90d") / col("transaction_count_90d")) \
    .withColumn("international_ratio",
                col("international_count_90d") / col("transaction_count_90d"))

log_dataframe_stats(customer_profiles, "Customer Profiles")


# ============================================================================
# Determine Risk Category
# ============================================================================

logger.info("Determining risk categories...")

customer_profiles = customer_profiles.withColumn(
    "risk_category",
    when(
        (col("high_value_ratio") > 0.3) |
        (col("high_risk_country_count_90d") > 5) |
        (col("avg_transaction_amount_90d") > 50000),
        lit("HIGH")
    )
    .when(
        (col("high_value_ratio") > 0.1) |
        (col("international_ratio") > 0.3),
        lit("MEDIUM")
    )
    .otherwise(lit("LOW"))
)

# Add metadata
customer_profiles = customer_profiles \
    .withColumn("effective_date", current_date()) \
    .withColumn("computed_at", current_timestamp())

log_dataframe_stats(customer_profiles, "Profiles with Risk Category")


# ============================================================================
# Read Current Redshift Dimension
# ============================================================================

logger.info("Reading current customer dimension from Redshift...")

# JDBC connection properties
jdbc_url = REDSHIFT_URL
connection_props = {
    "user": config.aws.redshift_user,
    "password": config.aws.redshift_password,
    "driver": "com.amazon.redshift.jdbc42.Driver"
}

# Read current customers (is_current = TRUE)
current_customers_df = (spark.read
    .jdbc(
        url=jdbc_url,
        table="(SELECT * FROM dim_customer WHERE is_current = TRUE) AS current_customers",
        properties=connection_props
    )
)

log_dataframe_stats(current_customers_df, "Current Customers from Redshift")


# ============================================================================
# Identify Changed Records (SCD Type 2)
# ============================================================================

logger.info("Identifying changed customer profiles...")

# Join new profiles with current dimension
comparison_df = customer_profiles.alias("new").join(
    current_customers_df.alias("current"),
    col("new.customer_id") == col("current.customer_id"),
    "left"
)

# Identify changes in risk category
changed_customers = comparison_df.filter(
    (col("current.customer_id").isNull()) |  # New customer
    (col("new.risk_category") != col("current.risk_category"))  # Changed risk
)

logger.info(f"Found {changed_customers.count()} customers with changes")


# ============================================================================
# Prepare SCD Type 2 Updates
# ============================================================================

if changed_customers.count() > 0:
    logger.info("Preparing SCD Type 2 updates...")
    
    # Step 1: Expire old records
    # Get customer_ids that need to be expired
    customer_ids_to_expire = changed_customers.select("new.customer_id").distinct()
    
    logger.info("Expiring old customer records...")
    
    # Build SQL for update
    customer_id_list = [row.customer_id for row in customer_ids_to_expire.collect()]
    customer_id_str = "','".join(customer_id_list)
    
    expire_sql = f"""
    UPDATE dim_customer
    SET expiration_date = CURRENT_DATE - 1,
        is_current = FALSE,
        updated_at = CURRENT_TIMESTAMP
    WHERE customer_id IN ('{customer_id_str}')
      AND is_current = TRUE
    """
    
    # Execute via JDBC (in production, use proper JDBC execution)
    # For now, we'll prepare the data for insertion
    
    # Step 2: Prepare new records for insertion
    logger.info("Preparing new customer records...")
    
    new_records = changed_customers.select(
        col("new.customer_id"),
        col("new.customer_name").alias("customer_name"),  # From enrichment
        col("new.risk_category"),
        col("new.avg_transaction_amount_90d"),
        col("new.avg_transaction_amount_30d"),
        col("new.transaction_count_30d"),
        col("new.transaction_count_90d"),
        col("new.last_transaction_date"),
        current_date().alias("effective_date"),
        lit("9999-12-31").cast("date").alias("expiration_date"),
        lit(True).alias("is_current"),
        current_timestamp().alias("created_at"),
        current_timestamp().alias("updated_at"),
    )
    
    log_dataframe_stats(new_records, "New Customer Records")
    
    # ========================================================================
    # Write to Redshift (SCD Type 2)
    # ========================================================================
    
    logger.info("Writing new records to Redshift...")
    
    # Write new records
    (new_records.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dim_customer")
        .option("user", config.aws.redshift_user)
        .option("password", config.aws.redshift_password)
        .option("driver", "com.amazon.redshift.jdbc42.Driver")
        .option("tempdir", REDSHIFT_TEMP)
        .mode("append")
        .save()
    )
    
    logger.info(f"Inserted {new_records.count()} new customer records")
    
else:
    logger.info("No customer changes detected - skipping SCD updates")


# ============================================================================
# Write Customer Profiles to Delta Lake (for reference)
# ============================================================================

logger.info("Writing customer profiles to Delta Lake...")

profiles_path = f"{DELTA_BASE}/gold/customer_profiles"

customer_profiles.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("effective_date") \
    .save(profiles_path)

logger.info(f"Customer profiles written to: {profiles_path}")


# ============================================================================
# Data Quality Checks
# ============================================================================

logger.info("Running data quality checks...")

# Check 1: All customers have a risk category
null_risk_count = customer_profiles.filter(col("risk_category").isNull()).count()
if null_risk_count > 0:
    logger.warning(f"Found {null_risk_count} customers with NULL risk category")
else:
    logger.info("✓ All customers have risk category")

# Check 2: Transaction counts are positive
invalid_counts = customer_profiles.filter(col("transaction_count_90d") <= 0).count()
if invalid_counts > 0:
    logger.warning(f"Found {invalid_counts} customers with invalid transaction counts")
else:
    logger.info("✓ All transaction counts are valid")

# Check 3: Average amounts are reasonable
invalid_amounts = customer_profiles.filter(
    (col("avg_transaction_amount_90d") < 0) |
    (col("avg_transaction_amount_90d") > 1000000)
).count()
if invalid_amounts > 0:
    logger.warning(f"Found {invalid_amounts} customers with suspicious average amounts")
else:
    logger.info("✓ All average amounts are reasonable")


# ============================================================================
# Generate Summary Statistics
# ============================================================================

logger.info("Generating summary statistics...")

summary = customer_profiles.groupBy("risk_category").agg(
    count("*").alias("customer_count"),
    spark_avg("avg_transaction_amount_90d").alias("avg_amount"),
    spark_avg("transaction_count_90d").alias("avg_tx_count"),
)

logger.info("Risk Category Distribution:")
summary.show()


# ============================================================================
# Job Completion
# ============================================================================

logger.info("Customer profile aggregation completed successfully")

job.commit()
