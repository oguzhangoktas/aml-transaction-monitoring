# Data Model Design

## Overview

The AML Transaction Monitoring system uses a **Medallion Architecture** for the data lake (Bronze → Silver → Gold) and a **Star Schema** for the data warehouse (Redshift). This document describes all data models, schemas, and transformations.

## Medallion Architecture (Delta Lake)

### Bronze Layer - Raw Data

**Purpose**: Store raw, immutable transaction data as-is from source systems.

#### Table: `bronze_transactions`

**Location**: `s3://aml-data-lake/delta/bronze/transactions/`

**Schema**:
```python
bronze_schema = StructType([
    # Transaction identifiers
    StructField("transaction_id", StringType(), nullable=False),
    StructField("transaction_ref", StringType(), nullable=True),
    
    # Customer information
    StructField("customer_id", StringType(), nullable=False),
    StructField("account_id", StringType(), nullable=False),
    
    # Transaction details
    StructField("amount", StringType(), nullable=False),  # Raw string from source
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
    
    # Timestamps
    StructField("transaction_timestamp", StringType(), nullable=False),  # Raw string
    StructField("value_date", StringType(), nullable=True),
    
    # Metadata
    StructField("source_system", StringType(), nullable=False),
    StructField("ingestion_timestamp", TimestampType(), nullable=False),
    StructField("kafka_partition", IntegerType(), nullable=True),
    StructField("kafka_offset", LongType(), nullable=True),
])
```

**Partitioning**:
```python
.partitionBy("ingestion_date")  # Format: YYYY-MM-DD
```

**Data Quality**:
- No transformations applied
- All data types as strings (except metadata)
- Duplicates allowed (idempotency handled in Silver)

**Retention**: 90 days in hot storage, then archived to S3 Glacier

---

### Silver Layer - Enriched Data

**Purpose**: Cleaned, validated, and enriched transactions ready for analysis.

#### Table: `silver_transactions`

**Location**: `s3://aml-data-lake/delta/silver/transactions/`

**Schema**:
```python
silver_schema = StructType([
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
    
    # Transaction details (validated & converted)
    StructField("amount", DecimalType(18, 2), nullable=False),
    StructField("amount_eur", DecimalType(18, 2), nullable=False),  # Converted to EUR
    StructField("currency", StringType(), nullable=False),
    StructField("exchange_rate", DecimalType(10, 6), nullable=True),
    StructField("transaction_type", StringType(), nullable=False),
    StructField("channel", StringType(), nullable=False),  # Defaulted if null
    
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
    
    # Timestamps (parsed & validated)
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
    StructField("data_quality_score", DecimalType(3, 2), nullable=True),  # 0.00-1.00
])
```

**Partitioning**:
```python
.partitionBy("transaction_date")  # Better for time-based queries
```

**Transformations Applied**:

1. **Data Type Conversion**
```python
# Amount parsing and validation
df.withColumn("amount", 
    when(col("amount").cast("decimal(18,2)").isNotNull(), 
         col("amount").cast("decimal(18,2)"))
    .otherwise(lit(0.0))
)
```

2. **Currency Conversion**
```python
# EUR conversion using daily exchange rates
df.join(exchange_rates, ["currency", "transaction_date"]) \
  .withColumn("amount_eur", col("amount") * col("exchange_rate"))
```

3. **Customer Enrichment**
```python
# Broadcast join with customer dimension
df.join(broadcast(customer_dim), "customer_id")
```

4. **Country Risk Scoring**
```python
# Join with FATF high-risk countries list
df.join(high_risk_countries, 
        df.transaction_country_code == high_risk_countries.country_code,
        "left") \
  .withColumn("is_high_risk_country", 
              when(high_risk_countries.country_code.isNotNull(), True)
              .otherwise(False))
```

**Data Quality Rules**:
- Amount must be > 0
- Customer must exist in dimension table
- Transaction timestamp within last 7 days
- Currency must be valid ISO 4217 code

**Deduplication**:
```python
# Use Delta merge for idempotency
silver_delta.alias("target").merge(
    new_data.alias("source"),
    "target.transaction_id = source.transaction_id"
).whenNotMatchedInsertAll().execute()
```

---

### Gold Layer - Aggregated Data

**Purpose**: Business-level aggregations optimized for analytics and reporting.

#### Table: `gold_alerts`

**Location**: `s3://aml-data-lake/delta/gold/alerts/`

**Schema**:
```python
gold_alerts_schema = StructType([
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
    StructField("risk_score", DecimalType(5, 2), nullable=False),  # 0.00-100.00
    StructField("confidence_score", DecimalType(3, 2), nullable=False),  # 0.00-1.00
    StructField("alert_severity", StringType(), nullable=False),  # LOW/MEDIUM/HIGH/CRITICAL
    
    # Transaction details (denormalized for performance)
    StructField("transaction_amount_eur", DecimalType(18, 2), nullable=False),
    StructField("transaction_country", StringType(), nullable=False),
    StructField("transaction_type", StringType(), nullable=False),
    
    # Customer context
    StructField("customer_risk_category", StringType(), nullable=False),
    StructField("customer_avg_transaction_amount", DecimalType(18, 2), nullable=True),
    StructField("customer_transaction_count_30d", IntegerType(), nullable=True),
    
    # Rule-specific data (JSON)
    StructField("rule_parameters", StringType(), nullable=True),  # JSON string
    StructField("rule_evaluation_details", StringType(), nullable=True),  # JSON string
    
    # Status tracking
    StructField("alert_status", StringType(), nullable=False),  # NEW/UNDER_REVIEW/CLOSED
    StructField("false_positive_flag", BooleanType(), nullable=True),
    StructField("investigation_notes", StringType(), nullable=True),
    
    # Metadata
    StructField("created_at", TimestampType(), nullable=False),
    StructField("updated_at", TimestampType(), nullable=True),
])
```

**Partitioning**:
```python
.partitionBy("alert_date", "rule_category")
```

#### Table: `gold_customer_daily_summary`

**Location**: `s3://aml-data-lake/delta/gold/customer_daily_summary/`

**Schema**:
```python
customer_summary_schema = StructType([
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
```

**Partitioning**:
```python
.partitionBy("summary_date")
```

**Update Strategy**: Daily batch job (Airflow)

---

## Star Schema (Amazon Redshift)

### Fact Table

#### `fact_alerts`

**Purpose**: Central fact table for all alerts generated by the system.

```sql
CREATE TABLE fact_alerts (
    -- Surrogate key
    alert_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Business key
    alert_id VARCHAR(100) NOT NULL UNIQUE,
    
    -- Foreign keys to dimensions
    customer_key INT NOT NULL,
    date_key INT NOT NULL,
    rule_key INT NOT NULL,
    account_key INT,
    
    -- Transaction reference
    transaction_id VARCHAR(100) NOT NULL,
    
    -- Measures (additive)
    transaction_amount_eur DECIMAL(18,2) NOT NULL,
    risk_score DECIMAL(5,2) NOT NULL,
    confidence_score DECIMAL(3,2) NOT NULL,
    
    -- Measures (semi-additive)
    customer_avg_transaction_amount DECIMAL(18,2),
    customer_transaction_count_30d INT,
    
    -- Degenerate dimensions
    alert_status VARCHAR(20) NOT NULL,
    alert_severity VARCHAR(20) NOT NULL,
    transaction_type VARCHAR(20) NOT NULL,
    transaction_country VARCHAR(3) NOT NULL,
    
    -- Flags
    false_positive_flag BOOLEAN DEFAULT FALSE,
    is_international BOOLEAN,
    is_high_risk_country BOOLEAN,
    
    -- Timestamps
    alert_timestamp TIMESTAMP NOT NULL,
    transaction_timestamp TIMESTAMP NOT NULL,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT GETDATE(),
    updated_at TIMESTAMP
)
DISTKEY(customer_key)
SORTKEY(date_key, alert_timestamp)
ENCODE AUTO;
```

**Distribution Strategy**: 
- `DISTKEY(customer_key)`: Co-locate alerts with customer dimension for fast joins

**Sort Strategy**:
- Primary: `date_key` for time-based queries
- Secondary: `alert_timestamp` for recency queries

**Indexing**:
```sql
-- No explicit indexes needed, Redshift uses sort keys
-- Consider interleaved sort key for multi-column queries
ALTER TABLE fact_alerts ALTER SORTKEY TO COMPOUND (date_key, customer_key);
```

---

### Dimension Tables

#### `dim_customer` (SCD Type 2)

**Purpose**: Customer master data with historical tracking.

```sql
CREATE TABLE dim_customer (
    -- Surrogate key
    customer_key INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Business key
    customer_id VARCHAR(100) NOT NULL,
    
    -- Attributes
    customer_name VARCHAR(200) NOT NULL,
    customer_segment VARCHAR(50),  -- RETAIL/CORPORATE/PRIVATE_BANKING
    risk_category VARCHAR(20) NOT NULL,  -- LOW/MEDIUM/HIGH
    
    -- Behavioral attributes (computed)
    avg_transaction_amount_30d DECIMAL(18,2),
    avg_transaction_amount_90d DECIMAL(18,2),
    transaction_count_30d INT,
    transaction_count_90d INT,
    alert_count_90d INT,
    
    -- PEP (Politically Exposed Person) flags
    is_pep BOOLEAN DEFAULT FALSE,
    pep_level VARCHAR(20),  -- DIRECT/ASSOCIATE/FAMILY
    
    -- Location
    customer_country_code VARCHAR(3),
    customer_country_name VARCHAR(100),
    
    -- Dates
    customer_since_date DATE,
    last_transaction_date DATE,
    
    -- SCD Type 2 fields
    effective_date DATE NOT NULL,
    expiration_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT GETDATE(),
    updated_at TIMESTAMP,
    
    -- Unique constraint for business key + effective date
    UNIQUE(customer_id, effective_date)
)
DISTSTYLE ALL  -- Broadcast to all nodes
ENCODE AUTO;
```

**SCD Type 2 Logic**:
When customer attributes change (e.g., risk_category: LOW → HIGH):

1. **Expire current record**:
```sql
UPDATE dim_customer
SET expiration_date = CURRENT_DATE - 1,
    is_current = FALSE
WHERE customer_id = '12345'
  AND is_current = TRUE;
```

2. **Insert new record**:
```sql
INSERT INTO dim_customer (
    customer_id, 
    customer_name, 
    risk_category,
    effective_date,
    is_current
)
VALUES (
    '12345',
    'John Doe',
    'HIGH',  -- Changed from LOW
    CURRENT_DATE,
    TRUE
);
```

**Query for current view**:
```sql
SELECT * 
FROM dim_customer
WHERE is_current = TRUE;
```

**Query for point-in-time view** (e.g., customer state on 2024-06-15):
```sql
SELECT * 
FROM dim_customer
WHERE customer_id = '12345'
  AND '2024-06-15' BETWEEN effective_date AND expiration_date;
```

#### `dim_rule`

**Purpose**: AML rule definitions.

```sql
CREATE TABLE dim_rule (
    -- Surrogate key
    rule_key INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Business key
    rule_id VARCHAR(50) NOT NULL UNIQUE,
    
    -- Attributes
    rule_name VARCHAR(200) NOT NULL,
    rule_description TEXT,
    rule_category VARCHAR(50) NOT NULL,  -- VELOCITY/VALUE/PATTERN/STRUCTURING
    
    -- Rule parameters
    threshold_value DECIMAL(18,2),
    threshold_currency VARCHAR(3),
    time_window_hours INT,
    
    -- Rule effectiveness metrics
    total_alerts_generated INT DEFAULT 0,
    true_positive_count INT DEFAULT 0,
    false_positive_count INT DEFAULT 0,
    precision_rate DECIMAL(5,4),  -- TP / (TP + FP)
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT GETDATE(),
    updated_at TIMESTAMP
)
DISTSTYLE ALL
ENCODE AUTO;
```

**Sample Rules**:
```sql
INSERT INTO dim_rule (rule_id, rule_name, rule_category, threshold_value, threshold_currency) VALUES
('RULE_HV_001', 'High Value Transaction', 'VALUE', 10000.00, 'EUR'),
('RULE_VEL_001', 'Rapid Movement 24h', 'VELOCITY', 15000.00, 'EUR'),
('RULE_STR_001', 'Structuring Detection', 'STRUCTURING', 9000.00, 'EUR'),
('RULE_GEO_001', 'High Risk Country', 'PATTERN', NULL, NULL),
('RULE_VEL_002', 'Unusual Transaction Frequency', 'VELOCITY', NULL, NULL);
```

#### `dim_date`

**Purpose**: Time dimension for time-based analytics.

```sql
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,  -- Format: YYYYMMDD
    date DATE NOT NULL UNIQUE,
    
    -- Date parts
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    week INT NOT NULL,
    day_of_month INT NOT NULL,
    day_of_week INT NOT NULL,  -- 1=Monday, 7=Sunday
    day_name VARCHAR(20) NOT NULL,
    
    -- Flags
    is_weekend BOOLEAN NOT NULL,
    is_business_day BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE,
    holiday_name VARCHAR(100),
    
    -- Fiscal calendar (if needed)
    fiscal_year INT,
    fiscal_quarter INT,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE ALL
SORTKEY(date_key)
ENCODE AUTO;
```

**Pre-populate** for 10 years (past 5 + future 5):
```python
# Generated via Python script, loaded once
date_range = pd.date_range(start='2020-01-01', end='2030-12-31')
```

#### `dim_account`

**Purpose**: Account dimension for customer accounts.

```sql
CREATE TABLE dim_account (
    -- Surrogate key
    account_key INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Business key
    account_id VARCHAR(100) NOT NULL UNIQUE,
    
    -- Foreign key
    customer_key INT NOT NULL,  -- FK to dim_customer
    
    -- Attributes
    account_type VARCHAR(50),  -- CHECKING/SAVINGS/INVESTMENT
    account_status VARCHAR(20),  -- ACTIVE/CLOSED/SUSPENDED
    
    -- Dates
    account_open_date DATE,
    account_close_date DATE,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT GETDATE(),
    updated_at TIMESTAMP,
    
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key)
)
DISTSTYLE ALL
ENCODE AUTO;
```

---

## Data Transformation Mappings

### Bronze → Silver Transformation

| Bronze Field | Silver Field | Transformation |
|--------------|--------------|----------------|
| `amount` (string) | `amount` (decimal) | `CAST(amount AS DECIMAL(18,2))` |
| `transaction_timestamp` (string) | `transaction_timestamp` (timestamp) | `TO_TIMESTAMP(transaction_timestamp, 'yyyy-MM-dd HH:mm:ss')` |
| N/A | `amount_eur` | `amount * exchange_rate` (lookup) |
| N/A | `customer_name` | Join with customer dimension |
| N/A | `is_high_risk_country` | Join with FATF list |
| `transaction_country` | `transaction_country_code` | ISO 3166-1 alpha-2 code lookup |

### Silver → Gold Transformation (Alerts)

```python
# Rule evaluation logic
alerts_df = silver_df \
    .withColumn("rule_high_value", 
                when(col("amount_eur") > 10000, True).otherwise(False)) \
    .withColumn("rule_velocity_24h",
                # Check if 24h rolling sum > 15000 EUR
                when(col("rolling_24h_amount") > 15000, True).otherwise(False)) \
    .filter(
        col("rule_high_value") | col("rule_velocity_24h")
    ) \
    .select(
        sha2(concat_ws("|", col("transaction_id"), col("rule_id")), 256).alias("alert_id"),
        col("transaction_id"),
        col("customer_id"),
        current_timestamp().alias("alert_timestamp"),
        # ... more fields
    )
```

### Delta Lake → Redshift Load

**Daily Incremental Load**:
```python
# Read yesterday's data from Gold layer
yesterday = datetime.now().date() - timedelta(days=1)
alerts_df = spark.read.format("delta") \
    .load("s3://bucket/delta/gold/alerts/") \
    .filter(col("alert_date") == yesterday)

# Write to staging table in Redshift
alerts_df.write \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", "staging.alerts") \
    .option("tempdir", "s3://bucket/temp/") \
    .mode("overwrite") \
    .save()

# Merge into fact table
spark.sql("""
    MERGE INTO fact_alerts AS target
    USING staging.alerts AS source
    ON target.alert_id = source.alert_id
    WHEN MATCHED THEN UPDATE SET ...
    WHEN NOT MATCHED THEN INSERT ...
""")
```

---

## Data Lineage

```
Source Systems (Core Banking)
    ↓
Kafka Topic: transactions-raw
    ↓
Glue Streaming Job: Validation & Enrichment
    ↓
Delta Lake Bronze: s3://.../bronze/transactions/
    ↓
Glue Streaming Job: Business Rules
    ↓
Delta Lake Silver: s3://.../silver/transactions/
    ↓
Glue Streaming Job: Alert Generation
    ↓
Delta Lake Gold: s3://.../gold/alerts/
    ↓
Airflow DAG: Daily Load
    ↓
Redshift Fact Table: fact_alerts
    ↓
BI Tools (Tableau, QuickSight)
```

---

## Performance Considerations

### Delta Lake Optimizations

1. **Z-Ordering** (for frequently filtered columns):
```sql
OPTIMIZE delta.`s3://bucket/delta/silver/transactions/`
ZORDER BY (customer_id, transaction_date);
```

2. **Vacuum** (clean up old versions):
```sql
VACUUM delta.`s3://bucket/delta/silver/transactions/` RETAIN 168 HOURS;
```

3. **File Compaction**:
```sql
OPTIMIZE delta.`s3://bucket/delta/silver/transactions/`
WHERE transaction_date >= CURRENT_DATE - INTERVAL 7 DAYS;
```

### Redshift Optimizations

1. **Analyze** (update statistics):
```sql
ANALYZE fact_alerts;
ANALYZE dim_customer;
```

2. **Vacuum** (reclaim space):
```sql
VACUUM DELETE ONLY fact_alerts;
```

3. **Distribution Key Validation**:
```sql
-- Check for data skew
SELECT customer_key, COUNT(*)
FROM fact_alerts
GROUP BY customer_key
ORDER BY COUNT(*) DESC
LIMIT 100;
```

---

## Summary

This data model supports:
- ✅ **Real-time analytics** (Delta Lake streaming writes)
- ✅ **Historical analysis** (SCD Type 2 in Redshift)
- ✅ **ACID transactions** (Delta Lake guarantees)
- ✅ **Query performance** (Star schema + proper indexing)
- ✅ **Scalability** (Partitioning + distribution strategies)
- ✅ **Auditability** (Full lineage + time travel)

The design follows **data engineering best practices** and is optimized for both streaming and batch workloads.
