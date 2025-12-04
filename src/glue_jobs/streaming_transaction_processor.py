"""
AWS Glue Streaming Job: Real-Time Transaction Processing

This job:
1. Reads transactions from Kafka (AWS MSK)
2. Validates and enriches data
3. Writes to Delta Lake (Bronze, Silver layers)
4. Evaluates AML rules
5. Generates alerts (Gold layer)

Author: Oğuzhan Göktaş
"""

import sys
from datetime import datetime
from decimal import Decimal

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, current_timestamp, to_timestamp,
    date_format, hour, dayofweek, broadcast, sum as spark_sum,
    count as spark_count, avg as spark_avg, window, concat_ws, sha2
)
from pyspark.sql.types import DecimalType
from delta.tables import DeltaTable

# Import custom modules (uploaded as extra-py-files)
import sys
sys.path.insert(0, '/tmp/')  # Glue extracts dependencies here

from common.schemas import (
    BRONZE_TRANSACTION_SCHEMA,
    SILVER_TRANSACTION_SCHEMA,
    GOLD_ALERT_SCHEMA
)
from common.config import Config
from common.utils import (
    setup_logger,
    create_or_get_delta_table,
    add_alert_id_column,
    log_dataframe_stats
)


# ============================================================================
# Configuration
# ============================================================================

logger = setup_logger(__name__)

# Get Glue job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'KAFKA_BOOTSTRAP_SERVERS',
    'KAFKA_TOPIC',
    'DELTA_LAKE_BASE_PATH',
    'CHECKPOINT_LOCATION',
])

config = Config.load(environment='production')

# Override with Glue parameters
KAFKA_BOOTSTRAP = args['KAFKA_BOOTSTRAP_SERVERS']
KAFKA_TOPIC = args['KAFKA_TOPIC']
DELTA_BASE = args['DELTA_LAKE_BASE_PATH']
CHECKPOINT = args['CHECKPOINT_LOCATION']

logger.info(f"Starting AML Transaction Processor")
logger.info(f"Kafka: {KAFKA_BOOTSTRAP} / Topic: {KAFKA_TOPIC}")
logger.info(f"Delta Lake: {DELTA_BASE}")


# ============================================================================
# Initialize Spark and Glue Context
# ============================================================================

spark = (SparkSession.builder
    .appName("AML-Transaction-Processor")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger.info(f"Spark version: {spark.version}")
logger.info(f"Glue version: {spark.conf.get('spark.glue.version', 'unknown')}")


# ============================================================================
# Load Reference Data (Broadcast)
# ============================================================================

def load_customer_dimension():
    """Load customer dimension for enrichment"""
    logger.info("Loading customer dimension...")
    
    # In production, this would come from Redshift or S3
    # For now, we'll create sample data
    customer_data = [
        ("C001", "John Doe", "RETAIL", "LOW", False, "2020-01-15"),
        ("C002", "Jane Smith", "CORPORATE", "MEDIUM", False, "2019-06-20"),
        ("C003", "Alice Johnson", "PRIVATE_BANKING", "HIGH", True, "2021-03-10"),
        # Add more customers as needed
    ]
    
    customer_df = spark.createDataFrame(
        customer_data,
        ["customer_id", "customer_name", "customer_segment", 
         "customer_risk_category", "is_pep", "customer_since_date"]
    )
    
    logger.info(f"Loaded {customer_df.count()} customers")
    return customer_df


def load_exchange_rates():
    """Load exchange rates for currency conversion"""
    logger.info("Loading exchange rates...")
    
    # Sample exchange rates (in production: load from S3/database)
    rates_data = [
        ("USD", 0.92),
        ("GBP", 1.17),
        ("CHF", 1.08),
        ("JPY", 0.0062),
        ("EUR", 1.0),
    ]
    
    rates_df = spark.createDataFrame(
        rates_data,
        ["currency", "rate_to_eur"]
    )
    
    logger.info(f"Loaded {rates_df.count()} exchange rates")
    return rates_df


def load_high_risk_countries():
    """Load FATF high-risk countries list"""
    logger.info("Loading high-risk countries...")
    
    # FATF high-risk countries (sample)
    high_risk_data = [
        ("AF", "Afghanistan", True),
        ("IR", "Iran", True),
        ("KP", "North Korea", True),
        ("SY", "Syria", True),
        ("YE", "Yemen", True),
    ]
    
    risk_df = spark.createDataFrame(
        high_risk_data,
        ["country_code", "country_name", "is_high_risk"]
    )
    
    logger.info(f"Loaded {risk_df.count()} high-risk countries")
    return risk_df


# Load and broadcast reference data
customer_dim = broadcast(load_customer_dimension())
exchange_rates = broadcast(load_exchange_rates())
high_risk_countries = broadcast(load_high_risk_countries())


# ============================================================================
# Read from Kafka
# ============================================================================

logger.info("Connecting to Kafka stream...")

raw_stream = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 10000)  # Rate limiting
    .option("kafka.security.protocol", "PLAINTEXT")
    .load()
)

logger.info("Kafka stream connected")


# ============================================================================
# Parse Kafka Messages (Bronze Layer)
# ============================================================================

from pyspark.sql.functions import from_json

# Parse JSON from Kafka value
transactions_bronze = (raw_stream
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), BRONZE_TRANSACTION_SCHEMA).alias("data"))
    .select("data.*")
    .withColumn("ingestion_timestamp", current_timestamp())
)

log_dataframe_stats(transactions_bronze, "Bronze Transactions")


# ============================================================================
# Write to Bronze Layer (Raw Data)
# ============================================================================

bronze_path = f"{DELTA_BASE}/bronze/transactions"
bronze_checkpoint = f"{CHECKPOINT}/bronze"

logger.info(f"Writing to Bronze layer: {bronze_path}")

bronze_query = (transactions_bronze
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", bronze_checkpoint)
    .partitionBy("ingestion_date")
    .start(bronze_path)
)


# ============================================================================
# Transform to Silver Layer (Enriched Data)
# ============================================================================

logger.info("Applying Silver transformations...")

transactions_silver = transactions_bronze

# 1. Type conversions and validations
transactions_silver = transactions_silver \
    .withColumn("amount", col("amount").cast(DecimalType(18, 2))) \
    .withColumn("transaction_timestamp", to_timestamp(col("transaction_timestamp"))) \
    .filter(col("amount") > 0) \
    .filter(col("transaction_timestamp").isNotNull())

# 2. Enrich with customer dimension
transactions_silver = transactions_silver.join(
    customer_dim,
    "customer_id",
    "left"
)

# 3. Currency conversion to EUR
transactions_silver = transactions_silver.join(
    exchange_rates,
    "currency",
    "left"
) \
.withColumn(
    "amount_eur",
    when(col("rate_to_eur").isNotNull(), 
         col("amount") * col("rate_to_eur"))
    .otherwise(col("amount"))  # Assume EUR if rate not found
)

# 4. Extract date/time components
transactions_silver = transactions_silver \
    .withColumn("transaction_date", date_format(col("transaction_timestamp"), "yyyy-MM-dd").cast("date")) \
    .withColumn("transaction_hour", hour(col("transaction_timestamp"))) \
    .withColumn("day_of_week", dayofweek(col("transaction_timestamp")))

# 5. Derive boolean flags
transactions_silver = transactions_silver \
    .withColumn("is_high_value", col("amount_eur") >= 10000) \
    .withColumn("is_unusual_time", 
                (col("transaction_hour") >= 22) | (col("transaction_hour") < 6)) \
    .withColumn("business_day_flag", col("day_of_week").between(2, 6))  # Mon-Fri

# 6. Join with high-risk countries
transactions_silver = transactions_silver.join(
    high_risk_countries,
    transactions_silver.transaction_country == high_risk_countries.country_code,
    "left"
) \
.withColumn("is_high_risk_country", 
            when(col("is_high_risk").isNotNull(), True).otherwise(False)) \
.drop("is_high_risk")

# 7. Add processing metadata
transactions_silver = transactions_silver \
    .withColumn("processing_timestamp", current_timestamp())

log_dataframe_stats(transactions_silver, "Silver Transactions")


# ============================================================================
# Write to Silver Layer
# ============================================================================

silver_path = f"{DELTA_BASE}/silver/transactions"
silver_checkpoint = f"{CHECKPOINT}/silver"

logger.info(f"Writing to Silver layer: {silver_path}")

silver_query = (transactions_silver
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", silver_checkpoint)
    .partitionBy("transaction_date")
    .start(silver_path)
)


# ============================================================================
# Apply AML Business Rules
# ============================================================================

logger.info("Evaluating AML business rules...")

def evaluate_high_value_rule(df):
    """Rule: High-value transaction (>€10,000)"""
    return df.withColumn(
        "rule_high_value",
        when(col("amount_eur") > 10000, True).otherwise(False)
    )


def evaluate_high_risk_country_rule(df):
    """Rule: Transaction to/from high-risk country"""
    return df.withColumn(
        "rule_high_risk_country",
        col("is_high_risk_country")
    )


def evaluate_unusual_time_rule(df):
    """Rule: Transaction at unusual time"""
    return df.withColumn(
        "rule_unusual_time",
        col("is_unusual_time")
    )


def evaluate_pep_rule(df):
    """Rule: PEP customer transaction"""
    return df.withColumn(
        "rule_pep_customer",
        when(col("is_pep") == True, True).otherwise(False)
    )


# Apply all rules
transactions_with_rules = transactions_silver
transactions_with_rules = evaluate_high_value_rule(transactions_with_rules)
transactions_with_rules = evaluate_high_risk_country_rule(transactions_with_rules)
transactions_with_rules = evaluate_unusual_time_rule(transactions_with_rules)
transactions_with_rules = evaluate_pep_rule(transactions_with_rules)


# ============================================================================
# Velocity-Based Rules (Stateful Aggregations)
# ============================================================================

logger.info("Computing velocity-based aggregations...")

# Define watermark for late data (allow 2 hours late)
transactions_with_rules = transactions_with_rules.withWatermark(
    "transaction_timestamp", 
    "2 hours"
)

# 24-hour rolling window aggregations per customer
velocity_agg = transactions_with_rules \
    .groupBy(
        window(col("transaction_timestamp"), "24 hours", "1 hour"),
        col("customer_id")
    ) \
    .agg(
        spark_sum("amount_eur").alias("rolling_24h_amount"),
        spark_count("*").alias("rolling_24h_count"),
        spark_avg("amount_eur").alias("rolling_24h_avg")
    )

# Join back velocity metrics
transactions_with_velocity = transactions_with_rules.join(
    velocity_agg,
    [
        transactions_with_rules.customer_id == velocity_agg.customer_id,
        transactions_with_rules.transaction_timestamp >= velocity_agg.window.start,
        transactions_with_rules.transaction_timestamp < velocity_agg.window.end
    ],
    "left"
).drop(velocity_agg.customer_id)

# Evaluate velocity rules
transactions_with_velocity = transactions_with_velocity \
    .withColumn(
        "rule_rapid_movement",
        when(col("rolling_24h_amount") > 15000, True).otherwise(False)
    ) \
    .withColumn(
        "rule_high_frequency",
        when(col("rolling_24h_count") > 10, True).otherwise(False)
    )


# ============================================================================
# Generate Alerts (Gold Layer)
# ============================================================================

logger.info("Generating alerts...")

# Filter transactions that triggered any rule
alerts = transactions_with_velocity.filter(
    col("rule_high_value") |
    col("rule_high_risk_country") |
    col("rule_unusual_time") |
    col("rule_pep_customer") |
    col("rule_rapid_movement") |
    col("rule_high_frequency")
)

# Calculate risk score
alerts = alerts.withColumn(
    "risk_score",
    (
        when(col("rule_high_value"), lit(20)).otherwise(lit(0)) +
        when(col("rule_high_risk_country"), lit(30)).otherwise(lit(0)) +
        when(col("rule_unusual_time"), lit(10)).otherwise(lit(0)) +
        when(col("rule_pep_customer"), lit(25)).otherwise(lit(0)) +
        when(col("rule_rapid_movement"), lit(25)).otherwise(lit(0)) +
        when(col("rule_high_frequency"), lit(15)).otherwise(lit(0))
    ).cast(DecimalType(5, 2))
)

# Determine alert severity
alerts = alerts.withColumn(
    "alert_severity",
    when(col("risk_score") >= 75, lit("CRITICAL"))
    .when(col("risk_score") >= 50, lit("HIGH"))
    .when(col("risk_score") >= 25, lit("MEDIUM"))
    .otherwise(lit("LOW"))
)

# Determine rule that triggered (primary rule)
alerts = alerts.withColumn(
    "rule_id",
    when(col("rule_high_value"), lit("RULE_HV_001"))
    .when(col("rule_rapid_movement"), lit("RULE_VEL_001"))
    .when(col("rule_high_risk_country"), lit("RULE_GEO_001"))
    .when(col("rule_pep_customer"), lit("RULE_PEP_001"))
    .when(col("rule_unusual_time"), lit("RULE_TIME_001"))
    .otherwise(lit("RULE_GEN_001"))
)

alerts = alerts.withColumn(
    "rule_name",
    when(col("rule_id") == "RULE_HV_001", lit("High Value Transaction"))
    .when(col("rule_id") == "RULE_VEL_001", lit("Rapid Movement"))
    .when(col("rule_id") == "RULE_GEO_001", lit("High Risk Country"))
    .when(col("rule_id") == "RULE_PEP_001", lit("PEP Customer"))
    .otherwise(lit("General Alert"))
)

# Generate alert_id
alerts = alerts.withColumn(
    "alert_id",
    sha2(concat_ws("|", col("transaction_id"), col("rule_id")), 256)
)

# Add alert metadata
alerts = alerts \
    .withColumn("alert_timestamp", current_timestamp()) \
    .withColumn("alert_date", date_format(current_timestamp(), "yyyy-MM-dd").cast("date")) \
    .withColumn("alert_status", lit("NEW")) \
    .withColumn("confidence_score", lit(0.85).cast(DecimalType(3, 2))) \
    .withColumn("rule_category", lit("VALUE")) \
    .withColumn("created_at", current_timestamp())

# Select final alert columns
alerts_final = alerts.select(
    "alert_id",
    "alert_timestamp",
    "alert_date",
    "transaction_id",
    "customer_id",
    "account_id",
    "rule_id",
    "rule_name",
    "rule_category",
    "risk_score",
    "confidence_score",
    "alert_severity",
    col("amount_eur").alias("transaction_amount_eur"),
    col("transaction_country_code").alias("transaction_country"),
    "transaction_type",
    "customer_risk_category",
    "alert_status",
    "created_at"
)

log_dataframe_stats(alerts_final, "Alerts")


# ============================================================================
# Write to Gold Layer (Alerts)
# ============================================================================

gold_alerts_path = f"{DELTA_BASE}/gold/alerts"
gold_checkpoint = f"{CHECKPOINT}/gold_alerts"

logger.info(f"Writing to Gold layer (Alerts): {gold_alerts_path}")

gold_query = (alerts_final
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", gold_checkpoint)
    .partitionBy("alert_date")
    .start(gold_alerts_path)
)


# ============================================================================
# Monitoring and Metrics
# ============================================================================

logger.info("Setting up query progress listeners...")

class QueryProgressLogger:
    """Log streaming query progress metrics"""
    
    def __init__(self, query_name):
        self.query_name = query_name
        self.logger = setup_logger(f"{__name__}.{query_name}")
    
    def on_query_progress(self, event):
        """Called when query makes progress"""
        progress = event.progress
        
        self.logger.info(f"Query: {self.query_name}")
        self.logger.info(f"  Batch: {progress.batchId}")
        self.logger.info(f"  Input Rows: {progress.numInputRows}")
        self.logger.info(f"  Processed Rows: {progress.processedRowsPerSecond}")
        self.logger.info(f"  Duration: {progress.durationMs.get('triggerExecution', 0)}ms")
        
        # Log to CloudWatch (in production)
        # cloudwatch.put_metric_data(...)


# Attach listeners (simplified for example)
logger.info("Streaming queries started successfully")


# ============================================================================
# Await Termination
# ============================================================================

logger.info("Streaming job running. Press Ctrl+C to stop...")

try:
    bronze_query.awaitTermination()
except KeyboardInterrupt:
    logger.info("Stopping streaming queries...")
    bronze_query.stop()
    silver_query.stop()
    gold_query.stop()
    logger.info("Streaming queries stopped")

job.commit()
logger.info("Job completed")
