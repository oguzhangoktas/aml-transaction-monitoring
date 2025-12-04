"""
Data schemas for AML Transaction Monitoring Pipeline

This module defines all schemas used across the pipeline for:
- Data validation
- Type enforcement
- Schema evolution tracking
- Documentation
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    TimestampType, DateType, IntegerType, BooleanType, LongType
)


# ============================================================================
# BRONZE LAYER SCHEMAS (Raw Data)
# ============================================================================

BRONZE_TRANSACTION_SCHEMA = StructType([
    # Transaction identifiers
    StructField("transaction_id", StringType(), nullable=False),
    StructField("transaction_ref", StringType(), nullable=True),
    
    # Customer information
    StructField("customer_id", StringType(), nullable=False),
    StructField("account_id", StringType(), nullable=False),
    
    # Transaction details (raw strings from source)
    StructField("amount", StringType(), nullable=False),
    StructField("currency", StringType(), nullable=False),
    StructField("transaction_type", StringType(), nullable=False),  # DEBIT/CREDIT
    StructField("channel", StringType(), nullable=True),  # ATM/ONLINE/BRANCH
    
    # Counterparty
    StructField("counterparty_account", StringType(), nullable=True),
    StructField("counterparty_name", StringType(), nullable=True),
    StructField("counterparty_bank", StringType(), nullable=True),
    
    # Location
    StructField("transaction_country", StringType(), nullable=True),
    StructField("transaction_city", StringType(), nullable=True),
    
    # Timestamps (raw strings)
    StructField("transaction_timestamp", StringType(), nullable=False),
    StructField("value_date", StringType(), nullable=True),
    
    # Metadata
    StructField("source_system", StringType(), nullable=False),
    StructField("ingestion_timestamp", TimestampType(), nullable=False),
    StructField("kafka_partition", IntegerType(), nullable=True),
    StructField("kafka_offset", LongType(), nullable=True),
])


# ============================================================================
# SILVER LAYER SCHEMAS (Enriched Data)
# ============================================================================

SILVER_TRANSACTION_SCHEMA = StructType([
    # Transaction identifiers
    StructField("transaction_id", StringType(), nullable=False),
    StructField("transaction_ref", StringType(), nullable=True),
    
    # Customer information (enriched)
    StructField("customer_id", StringType(), nullable=False),
    StructField("customer_name", StringType(), nullable=False),
    StructField("customer_risk_category", StringType(), nullable=False),  # LOW/MEDIUM/HIGH
    StructField("customer_segment", StringType(), nullable=True),  # RETAIL/CORPORATE/PEP
    StructField("customer_since_date", DateType(), nullable=True),
    StructField("account_id", StringType(), nullable=False),
    
    # Transaction details (validated & typed)
    StructField("amount", DecimalType(18, 2), nullable=False),
    StructField("amount_eur", DecimalType(18, 2), nullable=False),
    StructField("currency", StringType(), nullable=False),
    StructField("exchange_rate", DecimalType(10, 6), nullable=True),
    StructField("transaction_type", StringType(), nullable=False),
    StructField("channel", StringType(), nullable=False),
    
    # Counterparty (cleaned)
    StructField("counterparty_account", StringType(), nullable=True),
    StructField("counterparty_name", StringType(), nullable=True),
    StructField("counterparty_bank", StringType(), nullable=True),
    StructField("counterparty_country", StringType(), nullable=True),
    
    # Location (standardized)
    StructField("transaction_country_code", StringType(), nullable=False),  # ISO 3166-1
    StructField("transaction_country_name", StringType(), nullable=False),
    StructField("transaction_city", StringType(), nullable=True),
    StructField("is_high_risk_country", BooleanType(), nullable=False),
    
    # Timestamps (parsed)
    StructField("transaction_timestamp", TimestampType(), nullable=False),
    StructField("transaction_date", DateType(), nullable=False),
    StructField("transaction_hour", IntegerType(), nullable=False),
    StructField("value_date", DateType(), nullable=True),
    
    # Derived fields
    StructField("is_international", BooleanType(), nullable=False),
    StructField("is_high_value", BooleanType(), nullable=False),  # >10K EUR
    StructField("is_unusual_time", BooleanType(), nullable=False),  # 22:00-06:00
    StructField("business_day_flag", BooleanType(), nullable=False),
    
    # Metadata
    StructField("source_system", StringType(), nullable=False),
    StructField("ingestion_timestamp", TimestampType(), nullable=False),
    StructField("processing_timestamp", TimestampType(), nullable=False),
    StructField("data_quality_score", DecimalType(3, 2), nullable=True),
])


# ============================================================================
# GOLD LAYER SCHEMAS (Business Aggregates)
# ============================================================================

GOLD_ALERT_SCHEMA = StructType([
    # Alert identifiers
    StructField("alert_id", StringType(), nullable=False),
    StructField("alert_timestamp", TimestampType(), nullable=False),
    StructField("alert_date", DateType(), nullable=False),
    
    # Transaction reference
    StructField("transaction_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("account_id", StringType(), nullable=False),
    
    # Rule information
    StructField("rule_id", StringType(), nullable=False),
    StructField("rule_name", StringType(), nullable=False),
    StructField("rule_category", StringType(), nullable=False),  # VELOCITY/VALUE/PATTERN
    StructField("rule_triggered_at", TimestampType(), nullable=False),
    
    # Alert details
    StructField("risk_score", DecimalType(5, 2), nullable=False),
    StructField("confidence_score", DecimalType(3, 2), nullable=False),
    StructField("alert_severity", StringType(), nullable=False),  # LOW/MEDIUM/HIGH/CRITICAL
    
    # Transaction details (denormalized)
    StructField("transaction_amount_eur", DecimalType(18, 2), nullable=False),
    StructField("transaction_country", StringType(), nullable=False),
    StructField("transaction_type", StringType(), nullable=False),
    
    # Customer context
    StructField("customer_risk_category", StringType(), nullable=False),
    StructField("customer_avg_transaction_amount", DecimalType(18, 2), nullable=True),
    StructField("customer_transaction_count_30d", IntegerType(), nullable=True),
    
    # Rule-specific data
    StructField("rule_parameters", StringType(), nullable=True),  # JSON
    StructField("rule_evaluation_details", StringType(), nullable=True),  # JSON
    
    # Status
    StructField("alert_status", StringType(), nullable=False),
    StructField("false_positive_flag", BooleanType(), nullable=True),
    StructField("investigation_notes", StringType(), nullable=True),
    
    # Metadata
    StructField("created_at", TimestampType(), nullable=False),
    StructField("updated_at", TimestampType(), nullable=True),
])


GOLD_CUSTOMER_DAILY_SUMMARY_SCHEMA = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("summary_date", DateType(), nullable=False),
    
    # Transaction statistics
    StructField("transaction_count", IntegerType(), nullable=False),
    StructField("total_amount_eur", DecimalType(18, 2), nullable=False),
    StructField("avg_transaction_amount", DecimalType(18, 2), nullable=False),
    StructField("max_transaction_amount", DecimalType(18, 2), nullable=False),
    StructField("min_transaction_amount", DecimalType(18, 2), nullable=False),
    
    # Transaction patterns
    StructField("debit_count", IntegerType(), nullable=False),
    StructField("credit_count", IntegerType(), nullable=False),
    StructField("international_count", IntegerType(), nullable=False),
    StructField("high_risk_country_count", IntegerType(), nullable=False),
    
    # Channel usage
    StructField("atm_count", IntegerType(), nullable=False),
    StructField("online_count", IntegerType(), nullable=False),
    StructField("branch_count", IntegerType(), nullable=False),
    
    # Alerts
    StructField("alert_count", IntegerType(), nullable=False),
    StructField("high_severity_alert_count", IntegerType(), nullable=False),
    
    # Metadata
    StructField("computed_at", TimestampType(), nullable=False),
])


# ============================================================================
# DIMENSION SCHEMAS (for enrichment)
# ============================================================================

CUSTOMER_DIMENSION_SCHEMA = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("customer_name", StringType(), nullable=False),
    StructField("customer_risk_category", StringType(), nullable=False),
    StructField("customer_segment", StringType(), nullable=True),
    StructField("customer_since_date", DateType(), nullable=True),
    StructField("avg_transaction_amount_30d", DecimalType(18, 2), nullable=True),
    StructField("transaction_count_30d", IntegerType(), nullable=True),
    StructField("is_pep", BooleanType(), nullable=False),
    StructField("customer_country_code", StringType(), nullable=True),
])


EXCHANGE_RATE_SCHEMA = StructType([
    StructField("currency", StringType(), nullable=False),
    StructField("date", DateType(), nullable=False),
    StructField("rate_to_eur", DecimalType(10, 6), nullable=False),
])


HIGH_RISK_COUNTRY_SCHEMA = StructType([
    StructField("country_code", StringType(), nullable=False),  # ISO 3166-1
    StructField("country_name", StringType(), nullable=False),
    StructField("risk_level", StringType(), nullable=False),  # HIGH/MEDIUM
    StructField("fatf_listed", BooleanType(), nullable=False),
])


# ============================================================================
# KAFKA MESSAGE SCHEMAS
# ============================================================================

KAFKA_TRANSACTION_MESSAGE_SCHEMA = {
    "type": "object",
    "required": ["transaction_id", "customer_id", "amount", "currency", "timestamp"],
    "properties": {
        "transaction_id": {"type": "string"},
        "customer_id": {"type": "string"},
        "account_id": {"type": "string"},
        "amount": {"type": "number", "minimum": 0.01},
        "currency": {"type": "string", "minLength": 3, "maxLength": 3},
        "transaction_type": {"type": "string", "enum": ["DEBIT", "CREDIT"]},
        "channel": {"type": "string", "enum": ["ATM", "ONLINE", "BRANCH", "WIRE"]},
        "counterparty_account": {"type": "string"},
        "counterparty_name": {"type": "string"},
        "counterparty_bank": {"type": "string"},
        "transaction_country": {"type": "string"},
        "transaction_city": {"type": "string"},
        "timestamp": {"type": "string", "format": "date-time"},
        "value_date": {"type": "string", "format": "date"},
        "source_system": {"type": "string"},
    }
}


# ============================================================================
# SCHEMA REGISTRY
# ============================================================================

SCHEMA_REGISTRY = {
    "bronze_transactions": BRONZE_TRANSACTION_SCHEMA,
    "silver_transactions": SILVER_TRANSACTION_SCHEMA,
    "gold_alerts": GOLD_ALERT_SCHEMA,
    "gold_customer_daily_summary": GOLD_CUSTOMER_DAILY_SUMMARY_SCHEMA,
    "dim_customer": CUSTOMER_DIMENSION_SCHEMA,
    "dim_exchange_rates": EXCHANGE_RATE_SCHEMA,
    "dim_high_risk_countries": HIGH_RISK_COUNTRY_SCHEMA,
}


def get_schema(schema_name: str) -> StructType:
    """
    Get schema by name from registry
    
    Args:
        schema_name: Name of the schema
        
    Returns:
        PySpark StructType schema
        
    Raises:
        KeyError: If schema not found
    """
    if schema_name not in SCHEMA_REGISTRY:
        raise KeyError(f"Schema '{schema_name}' not found in registry. "
                      f"Available schemas: {list(SCHEMA_REGISTRY.keys())}")
    return SCHEMA_REGISTRY[schema_name]


def print_schema(schema_name: str):
    """Print schema in readable format"""
    schema = get_schema(schema_name)
    print(f"\n{'='*60}")
    print(f"Schema: {schema_name}")
    print(f"{'='*60}")
    for field in schema.fields:
        nullable = "NULL" if field.nullable else "NOT NULL"
        print(f"  {field.name:<30} {str(field.dataType):<20} {nullable}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    # Print all schemas
    for schema_name in SCHEMA_REGISTRY.keys():
        print_schema(schema_name)
