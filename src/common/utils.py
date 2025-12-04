"""
Utility functions for AML Transaction Monitoring Pipeline
"""

import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import hashlib
import json

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, sha2, concat_ws
from delta.tables import DeltaTable


# ============================================================================
# Logging Setup
# ============================================================================

def setup_logger(name: str, log_level: str = "INFO") -> logging.Logger:
    """
    Set up logger with consistent formatting
    
    Args:
        name: Logger name
        log_level: Log level (DEBUG, INFO, WARNING, ERROR)
        
    Returns:
        Configured logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger


# ============================================================================
# Delta Lake Utilities
# ============================================================================

def create_or_get_delta_table(
    spark: SparkSession,
    path: str,
    schema: Any,
    partition_by: Optional[list] = None
) -> DeltaTable:
    """
    Create Delta table if not exists, otherwise return existing table
    
    Args:
        spark: SparkSession
        path: Delta table path
        schema: Table schema (StructType)
        partition_by: List of columns to partition by
        
    Returns:
        DeltaTable instance
    """
    if DeltaTable.isDeltaTable(spark, path):
        return DeltaTable.forPath(spark, path)
    
    # Create empty DataFrame with schema
    empty_df = spark.createDataFrame([], schema)
    
    # Write to create table
    writer = empty_df.write.format("delta").mode("overwrite")
    
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    
    writer.save(path)
    
    return DeltaTable.forPath(spark, path)


def optimize_delta_table(
    spark: SparkSession,
    path: str,
    zorder_by: Optional[list] = None,
    where: Optional[str] = None
):
    """
    Optimize Delta table (compact files and optionally z-order)
    
    Args:
        spark: SparkSession
        path: Delta table path
        zorder_by: List of columns to z-order by
        where: Optional WHERE clause to optimize specific partitions
    """
    logger = setup_logger(__name__)
    
    optimize_sql = f"OPTIMIZE delta.`{path}`"
    
    if where:
        optimize_sql += f" WHERE {where}"
    
    logger.info(f"Optimizing Delta table: {path}")
    spark.sql(optimize_sql)
    
    if zorder_by:
        zorder_cols = ", ".join(zorder_by)
        zorder_sql = f"OPTIMIZE delta.`{path}` ZORDER BY ({zorder_cols})"
        if where:
            zorder_sql += f" WHERE {where}"
        logger.info(f"Z-ordering by: {zorder_cols}")
        spark.sql(zorder_sql)


def vacuum_delta_table(
    spark: SparkSession,
    path: str,
    retention_hours: int = 168  # 7 days
):
    """
    Vacuum Delta table to remove old files
    
    Args:
        spark: SparkSession
        path: Delta table path
        retention_hours: Retention period in hours
    """
    logger = setup_logger(__name__)
    logger.info(f"Vacuuming Delta table: {path} (retention: {retention_hours}h)")
    
    spark.sql(f"VACUUM delta.`{path}` RETAIN {retention_hours} HOURS")


def merge_into_delta(
    delta_table: DeltaTable,
    source_df: DataFrame,
    merge_condition: str,
    match_columns: Optional[Dict[str, str]] = None,
    insert_columns: Optional[Dict[str, str]] = None
):
    """
    Perform MERGE operation on Delta table (upsert)
    
    Args:
        delta_table: Target Delta table
        source_df: Source DataFrame
        merge_condition: Merge condition (e.g., "target.id = source.id")
        match_columns: Columns to update when matched
        insert_columns: Columns to insert when not matched
    """
    logger = setup_logger(__name__)
    
    merge_builder = delta_table.alias("target").merge(
        source_df.alias("source"),
        merge_condition
    )
    
    if match_columns:
        merge_builder = merge_builder.whenMatchedUpdate(set=match_columns)
    else:
        merge_builder = merge_builder.whenMatchedUpdateAll()
    
    if insert_columns:
        merge_builder = merge_builder.whenNotMatchedInsert(values=insert_columns)
    else:
        merge_builder = merge_builder.whenNotMatchedInsertAll()
    
    logger.info(f"Executing MERGE operation with condition: {merge_condition}")
    merge_builder.execute()


# ============================================================================
# Data Quality Utilities
# ============================================================================

def calculate_completeness(df: DataFrame, required_columns: list) -> float:
    """
    Calculate completeness score for required columns
    
    Args:
        df: DataFrame to check
        required_columns: List of column names that must be non-null
        
    Returns:
        Completeness score (0.0 to 1.0)
    """
    total_rows = df.count()
    if total_rows == 0:
        return 0.0
    
    complete_rows = df
    for col_name in required_columns:
        complete_rows = complete_rows.filter(col(col_name).isNotNull())
    
    complete_count = complete_rows.count()
    return complete_count / total_rows


def add_data_quality_metrics(df: DataFrame) -> DataFrame:
    """
    Add data quality score column to DataFrame
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with data_quality_score column
    """
    # Count null fields
    null_count = sum([
        col(c).isNull().cast("int")
        for c in df.columns
        if c not in ["data_quality_score"]
    ])
    
    total_fields = len(df.columns) - 1  # Exclude data_quality_score itself
    
    # Calculate score: (total_fields - null_count) / total_fields
    quality_score = (lit(total_fields) - null_count) / lit(total_fields)
    
    return df.withColumn("data_quality_score", quality_score.cast("decimal(3,2)"))


def validate_schema(df: DataFrame, expected_schema: Any) -> tuple[bool, list]:
    """
    Validate DataFrame schema matches expected schema
    
    Args:
        df: DataFrame to validate
        expected_schema: Expected StructType schema
        
    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []
    
    # Check column names
    expected_cols = set(field.name for field in expected_schema.fields)
    actual_cols = set(df.columns)
    
    missing_cols = expected_cols - actual_cols
    extra_cols = actual_cols - expected_cols
    
    if missing_cols:
        errors.append(f"Missing columns: {missing_cols}")
    
    if extra_cols:
        errors.append(f"Extra columns: {extra_cols}")
    
    # Check data types for common columns
    for field in expected_schema.fields:
        if field.name in df.columns:
            actual_type = str(df.schema[field.name].dataType)
            expected_type = str(field.dataType)
            if actual_type != expected_type:
                errors.append(
                    f"Column '{field.name}' type mismatch: "
                    f"expected {expected_type}, got {actual_type}"
                )
    
    return len(errors) == 0, errors


# ============================================================================
# Hash & Identifier Utilities
# ============================================================================

def generate_alert_id(transaction_id: str, rule_id: str) -> str:
    """
    Generate deterministic alert ID
    
    Args:
        transaction_id: Transaction identifier
        rule_id: Rule identifier
        
    Returns:
        SHA-256 hash as alert ID
    """
    composite = f"{transaction_id}|{rule_id}"
    return hashlib.sha256(composite.encode()).hexdigest()


def add_alert_id_column(df: DataFrame) -> DataFrame:
    """
    Add alert_id column to DataFrame
    
    Args:
        df: DataFrame with transaction_id and rule_id columns
        
    Returns:
        DataFrame with alert_id column
    """
    return df.withColumn(
        "alert_id",
        sha2(concat_ws("|", col("transaction_id"), col("rule_id")), 256)
    )


# ============================================================================
# Time & Date Utilities
# ============================================================================

def get_date_range(days_back: int) -> tuple[str, str]:
    """
    Get date range (start_date, end_date) for given days back
    
    Args:
        days_back: Number of days to go back
        
    Returns:
        Tuple of (start_date, end_date) as strings (YYYY-MM-DD)
    """
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days_back)
    
    return str(start_date), str(end_date)


def is_business_day(dt: datetime) -> bool:
    """
    Check if given datetime is a business day (Monday-Friday)
    
    Args:
        dt: Datetime to check
        
    Returns:
        True if business day, False otherwise
    """
    return dt.weekday() < 5  # 0=Monday, 6=Sunday


def is_unusual_time(dt: datetime) -> bool:
    """
    Check if transaction time is unusual (22:00-06:00)
    
    Args:
        dt: Datetime to check
        
    Returns:
        True if unusual time, False otherwise
    """
    hour = dt.hour
    return hour >= 22 or hour < 6


# ============================================================================
# JSON Utilities
# ============================================================================

def dict_to_json_string(d: Dict[str, Any]) -> str:
    """
    Convert dictionary to JSON string
    
    Args:
        d: Dictionary
        
    Returns:
        JSON string
    """
    return json.dumps(d, default=str)


def json_string_to_dict(s: str) -> Dict[str, Any]:
    """
    Convert JSON string to dictionary
    
    Args:
        s: JSON string
        
    Returns:
        Dictionary
    """
    return json.loads(s)


# ============================================================================
# Monitoring Utilities
# ============================================================================

def log_dataframe_stats(df: DataFrame, name: str):
    """
    Log statistics about a DataFrame
    
    Args:
        df: DataFrame to analyze
        name: Name for logging
    """
    logger = setup_logger(__name__)
    
    row_count = df.count()
    col_count = len(df.columns)
    
    logger.info(f"DataFrame Stats - {name}:")
    logger.info(f"  Rows: {row_count:,}")
    logger.info(f"  Columns: {col_count}")
    
    if row_count > 0:
        logger.info(f"  Schema: {df.schema.simpleString()}")


def measure_execution_time(func):
    """
    Decorator to measure function execution time
    """
    def wrapper(*args, **kwargs):
        logger = setup_logger(__name__)
        start_time = datetime.now()
        
        logger.info(f"Starting: {func.__name__}")
        result = func(*args, **kwargs)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info(f"Completed: {func.__name__} (Duration: {duration:.2f}s)")
        return result
    
    return wrapper


# ============================================================================
# Currency Utilities
# ============================================================================

def get_eur_conversion_rate(currency: str, rates_dict: Dict[str, float]) -> float:
    """
    Get EUR conversion rate for given currency
    
    Args:
        currency: Currency code (ISO 4217)
        rates_dict: Dictionary of currency -> rate mappings
        
    Returns:
        Conversion rate to EUR
    """
    if currency == "EUR":
        return 1.0
    
    return rates_dict.get(currency, 1.0)  # Default to 1.0 if not found


# ============================================================================
# Constants
# ============================================================================

HIGH_RISK_COUNTRIES = [
    "AF",  # Afghanistan
    "IR",  # Iran
    "KP",  # North Korea
    "SY",  # Syria
    # Add more based on FATF list
]


TRANSACTION_CHANNELS = ["ATM", "ONLINE", "BRANCH", "WIRE", "MOBILE"]


RISK_CATEGORIES = ["LOW", "MEDIUM", "HIGH"]


CUSTOMER_SEGMENTS = ["RETAIL", "CORPORATE", "PRIVATE_BANKING", "PEP"]


ALERT_SEVERITIES = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]


ALERT_STATUSES = ["NEW", "UNDER_REVIEW", "ESCALATED", "CLOSED", "FALSE_POSITIVE"]


if __name__ == "__main__":
    # Test utilities
    print("Testing utilities...")
    
    # Test alert ID generation
    alert_id = generate_alert_id("TXN123", "RULE001")
    print(f"Generated alert ID: {alert_id}")
    
    # Test date range
    start, end = get_date_range(30)
    print(f"Date range (30 days): {start} to {end}")
    
    # Test time checks
    dt = datetime(2024, 11, 15, 23, 30)  # Friday 23:30
    print(f"Is business day? {is_business_day(dt)}")
    print(f"Is unusual time? {is_unusual_time(dt)}")
