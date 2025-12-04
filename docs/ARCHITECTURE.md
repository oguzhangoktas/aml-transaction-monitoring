# Architecture Deep Dive

## System Overview

The AML Transaction Monitoring System is designed as a modern, cloud-native data engineering pipeline that processes financial transactions in real-time to detect suspicious activities.

## Design Principles

1. **Medallion Architecture**: Bronze (raw) → Silver (enriched) → Gold (aggregated)
2. **Lambda Architecture Pattern**: Real-time + batch processing
3. **Event-Driven Design**: Kafka as central nervous system
4. **Idempotent Processing**: Exactly-once semantics
5. **Schema Evolution**: Forward/backward compatibility

## Data Flow Architecture

### Real-Time Path (Hot Path)

```
Transaction Source
    │
    ├─→ Kafka Topic: transactions-raw
    │       │
    │       ├─→ AWS Glue Streaming Job
    │       │       │
    │       │       ├─→ Schema Validation
    │       │       ├─→ Enrichment (Customer lookup)
    │       │       ├─→ Rules Engine Evaluation
    │       │       └─→ Alert Generation
    │       │
    │       ├─→ Delta Lake (Bronze Layer)
    │       │   s3://bucket/delta/bronze/transactions/
    │       │
    │       ├─→ Delta Lake (Silver Layer)
    │       │   s3://bucket/delta/silver/enriched_transactions/
    │       │
    │       └─→ Delta Lake (Gold Layer)
    │           s3://bucket/delta/gold/alerts/
    │
    └─→ Kafka Topic: alerts-high-priority
            │
            └─→ Alert Notification System
```

### Batch Path (Cold Path)

```
Daily Schedule (Airflow)
    │
    ├─→ Profile Aggregation Job
    │   │
    │   ├─→ Read: Silver transactions (last 90 days)
    │   ├─→ Compute: Customer behavior profiles
    │   └─→ Write: Redshift (dim_customer_profile - SCD Type 2)
    │
    ├─→ Alert Report Job
    │   │
    │   ├─→ Read: Gold alerts (daily)
    │   ├─→ Aggregate: By rule, region, risk level
    │   └─→ Write: Redshift (fact_daily_alerts)
    │
    └─→ Data Quality Job
        │
        ├─→ Completeness checks
        ├─→ Referential integrity
        ├─→ Business rule validation
        └─→ Alert on failures
```

## Component Architecture

### 1. Ingestion Layer (Apache Kafka)

**Technology**: AWS Managed Streaming for Kafka (MSK)

**Topics**:
- `transactions-raw`: Raw transaction events (retention: 7 days)
- `alerts-high-priority`: Critical alerts (retention: 30 days)
- `alerts-medium-priority`: Medium risk alerts (retention: 30 days)

**Configuration**:
```python
topic_config = {
    'partitions': 12,  # For parallelism
    'replication_factor': 3,  # For reliability
    'min_insync_replicas': 2,  # For durability
    'compression_type': 'snappy',  # For efficiency
}
```

**Partitioning Strategy**: Hash by customer_id for stateful processing

### 2. Processing Layer (AWS Glue + Spark)

#### Streaming Job: `streaming_transaction_processor.py`

**Resource Configuration**:
```
Glue Version: 4.0
Worker Type: G.1X (4 vCPU, 16 GB RAM)
Number of Workers: 10 (auto-scales to 50)
Max Capacity: 50 DPU
```

**Processing Logic**:

1. **Schema Validation**
   ```python
   validated_df = df.select(
       col("transaction_id"),
       col("customer_id"),
       col("amount").cast("decimal(18,2)"),
       col("timestamp").cast("timestamp"),
       # ... all fields with type validation
   )
   ```

2. **Enrichment**
   ```python
   # Broadcast join with customer dimension
   enriched_df = validated_df.join(
       broadcast(customer_dim),
       "customer_id"
   )
   ```

3. **Rules Evaluation**
   ```python
   alerts_df = enriched_df.select(
       "*",
       evaluate_high_value_rule(...).alias("rule_high_value"),
       evaluate_velocity_rule(...).alias("rule_velocity"),
       evaluate_structuring_rule(...).alias("rule_structuring")
   ).filter(
       col("rule_high_value") | col("rule_velocity") | col("rule_structuring")
   )
   ```

4. **State Management (for velocity rules)**
   ```python
   # Maintain 24-hour rolling window per customer
   windowed_df = enriched_df.groupBy(
       window(col("timestamp"), "24 hours", "1 hour"),
       col("customer_id")
   ).agg(
       sum("amount").alias("rolling_24h_amount"),
       count("*").alias("rolling_24h_count")
   )
   ```

**Checkpointing**: S3 location for exactly-once semantics
```
s3://bucket/checkpoints/streaming_processor/
```

### 3. Storage Layer (Delta Lake)

#### Bronze Layer (Raw)
**Path**: `s3://bucket/delta/bronze/transactions/`

**Schema**:
```python
bronze_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("amount", DecimalType(18, 2), False),
    StructField("currency", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("ingestion_time", TimestampType(), False),
    # ... more fields
])
```

**Partitioning**: By date for efficient querying
```python
df.write.format("delta") \
    .partitionBy("date") \
    .mode("append") \
    .save("s3://bucket/delta/bronze/transactions/")
```

**Optimization**:
```python
# Daily optimization job (Airflow)
spark.sql("""
    OPTIMIZE delta.`s3://bucket/delta/bronze/transactions/`
    WHERE date >= current_date() - INTERVAL 7 DAYS
""")

# Z-ordering for customer queries
spark.sql("""
    OPTIMIZE delta.`s3://bucket/delta/bronze/transactions/`
    ZORDER BY (customer_id)
""")
```

#### Silver Layer (Enriched)
**Path**: `s3://bucket/delta/silver/enriched_transactions/`

**Transformations**:
- Schema validation enforced
- NULL handling (defaulting/rejection)
- Data type conversions
- Customer dimension joined
- Currency conversion to EUR
- Risk scoring

**Delta Features Used**:
- **ACID Transactions**: No partial writes
- **Time Travel**: Query historical data
- **Schema Evolution**: Add columns without breaking
- **Merge Operations**: Upserts for late-arriving data

```python
# Handle late-arriving data
delta_table.alias("target").merge(
    late_data.alias("source"),
    "target.transaction_id = source.transaction_id"
).whenMatchedUpdate(
    condition="source.timestamp > target.timestamp",
    set={
        "amount": "source.amount",
        "updated_at": "current_timestamp()"
    }
).whenNotMatchedInsert(
    values={
        # ... all columns
    }
).execute()
```

#### Gold Layer (Aggregated)
**Path**: `s3://bucket/delta/gold/alerts/`

**Tables**:
1. `alerts`: Individual alert records
2. `customer_daily_summary`: Aggregated customer behavior
3. `rule_effectiveness`: Alert precision metrics

### 4. Data Warehouse (Amazon Redshift)

#### Star Schema Design

**Fact Table**: `fact_alerts`
```sql
CREATE TABLE fact_alerts (
    alert_id BIGINT IDENTITY(1,1),
    alert_key VARCHAR(100) NOT NULL,  -- Business key
    customer_key INT NOT NULL,  -- FK to dim_customer
    date_key INT NOT NULL,  -- FK to dim_date
    rule_key INT NOT NULL,  -- FK to dim_rule
    transaction_id VARCHAR(100) NOT NULL,
    amount DECIMAL(18,2) NOT NULL,
    risk_score DECIMAL(5,2),
    alert_status VARCHAR(20),
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (alert_id)
)
DISTKEY(customer_key)
SORTKEY(date_key, created_at);
```

**Dimension Tables**:

1. `dim_customer` (SCD Type 2)
```sql
CREATE TABLE dim_customer (
    customer_key INT IDENTITY(1,1),
    customer_id VARCHAR(100) NOT NULL,
    customer_name VARCHAR(200),
    risk_category VARCHAR(20),
    avg_transaction_amount DECIMAL(18,2),
    transaction_count_90d INT,
    -- SCD Type 2 fields
    effective_date DATE NOT NULL,
    expiration_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (customer_key)
)
DISTSTYLE ALL;  -- Broadcast for joins
```

2. `dim_rule`
```sql
CREATE TABLE dim_rule (
    rule_key INT IDENTITY(1,1),
    rule_id VARCHAR(50) NOT NULL,
    rule_name VARCHAR(200),
    rule_category VARCHAR(50),
    threshold_value DECIMAL(18,2),
    PRIMARY KEY (rule_key)
)
DISTSTYLE ALL;
```

3. `dim_date`
```sql
CREATE TABLE dim_date (
    date_key INT,
    date DATE NOT NULL,
    year INT,
    quarter INT,
    month INT,
    week INT,
    day_of_week INT,
    is_weekend BOOLEAN,
    is_business_day BOOLEAN,
    PRIMARY KEY (date_key)
)
DISTSTYLE ALL;
```

**Loading Strategy**:
- **Incremental loads**: Delta Lake → Redshift (daily)
- **COPY command**: Efficient bulk loading from S3
- **SCD Type 2 logic**: Implemented in Glue job

```python
# SCD Type 2 update logic (pseudo-code)
# 1. Expire current record
# 2. Insert new record with effective_date = today
```

### 5. Orchestration Layer (Apache Airflow)

**DAGs**:

1. **`daily_profile_refresh`** (runs at 02:00 UTC)
   - Reads last 90 days of transactions
   - Computes customer profiles
   - Updates Redshift dimensions (SCD Type 2)

2. **`weekly_alert_report`** (runs Sunday 03:00 UTC)
   - Aggregates weekly alert metrics
   - Loads to fact tables
   - Triggers report generation

3. **`data_quality_checks`** (runs every 4 hours)
   - Schema validation
   - Completeness checks (>99% non-null)
   - Referential integrity
   - Business rule validation
   - Sends alerts on failures

**Airflow Configuration**:
```python
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}
```

## Data Quality Framework

### Validation Layers

1. **Ingestion Validation** (Kafka)
   - JSON schema validation
   - Required field presence
   - Reject malformed messages

2. **Processing Validation** (Spark)
   - Data type enforcement
   - Range checks (e.g., amount > 0)
   - Referential integrity
   - Business rule validation

3. **Storage Validation** (Delta Lake)
   - Schema evolution checks
   - Duplicate detection
   - Completeness metrics

4. **Consumption Validation** (Redshift)
   - Aggregate reconciliation
   - Row count validation
   - Historical comparison

### Quality Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Completeness | 99.9% | Non-null critical fields |
| Accuracy | 99.9% | Rule evaluation correctness |
| Timeliness | <3s P95 | Ingestion to alert latency |
| Consistency | 100% | Cross-system reconciliation |
| Uniqueness | 100% | No duplicate transactions |

## Scalability Design

### Horizontal Scaling
- **Kafka**: Add partitions (12 → 24)
- **Glue**: Increase workers (10 → 50 auto-scaling)
- **Redshift**: Add nodes (2 → 4)

### Vertical Scaling
- **Glue Worker Type**: G.1X → G.2X (more memory)
- **Redshift Node Type**: dc2.large → dc2.8xlarge

### Current Capacity
- **Throughput**: 1,200 TPS (peak)
- **Daily Volume**: 5M transactions
- **Storage**: ~500GB/month (compressed)

### Scale Targets
- **6 months**: 3,000 TPS / 12M daily
- **12 months**: 5,000 TPS / 20M daily

## Monitoring & Observability

### CloudWatch Metrics

**Glue Streaming Job**:
- `glue.driver.aggregate.numRecordsRead`: Records processed
- `glue.driver.aggregate.numRecordsWritten`: Records written
- `glue.driver.streaming.latency`: Processing latency
- `glue.driver.system.cpuUtilization`: CPU usage
- `glue.driver.system.memoryUtilization`: Memory usage

**Custom Metrics**:
```python
# Published from Glue job
cloudwatch.put_metric_data(
    Namespace='AML/Pipeline',
    MetricData=[
        {
            'MetricName': 'AlertsGenerated',
            'Value': alert_count,
            'Unit': 'Count',
            'Timestamp': datetime.now()
        }
    ]
)
```

### Alerting Rules
1. **Processing lag** > 5 minutes → Critical
2. **Error rate** > 1% → Warning
3. **Data quality** < 99% → Critical
4. **Glue job failures** → Critical

## Security Architecture

### Encryption
- **At Rest**: S3 SSE-KMS, Redshift encryption
- **In Transit**: TLS 1.2+ for all connections
- **Kafka**: SSL/TLS encryption

### Access Control
- **IAM Roles**: Least privilege for Glue/Airflow
- **S3 Bucket Policies**: Restricted access
- **Redshift**: Row-level security for sensitive data
- **Secrets Manager**: Database credentials

### Audit Trail
- **Delta Lake**: Transaction log for all changes
- **CloudTrail**: All AWS API calls logged
- **Redshift**: Query monitoring for compliance

## Disaster Recovery

### Backup Strategy
- **Delta Lake**: Time travel (30 days retention)
- **Redshift**: Automated snapshots (daily)
- **Kafka**: Multi-AZ replication (RF=3)

### Recovery Procedures
- **RTO**: 4 hours (Recovery Time Objective)
- **RPO**: 1 hour (Recovery Point Objective)
- **DR Region**: Secondary AWS region (standby)

## Cost Optimization

### Storage Lifecycle
```python
# S3 Lifecycle Policy
bronze_lifecycle = {
    'Rules': [{
        'Id': 'bronze-to-glacier',
        'Prefix': 'delta/bronze/',
        'Status': 'Enabled',
        'Transitions': [{
            'Days': 90,
            'StorageClass': 'GLACIER'
        }]
    }]
}
```

### Compute Optimization
- **Glue**: Auto-scaling based on load
- **Redshift**: Pause/resume for dev environments
- **S3**: Intelligent-Tiering storage class

### Estimated Monthly Costs (at 5M tx/day)

| Service | Cost |
|---------|------|
| MSK (m5.large x 3) | $180 |
| Glue (avg 20 DPU-hours/day) | $120 |
| S3 (1TB storage + requests) | $40 |
| Redshift (dc2.large x 2) | $360 |
| Data Transfer | $30 |
| CloudWatch | $20 |
| **Total** | **~$750/month** |

---

## Technical Decisions & Trade-offs

### Why Delta Lake over Parquet?
✅ ACID transactions, time travel, schema evolution
❌ Slightly higher storage overhead (~5%)

### Why AWS Glue over EMR?
✅ Serverless, auto-scaling, no cluster management
❌ Less control over Spark configuration

### Why Redshift over Athena?
✅ Better performance for complex joins, BI tool integration
❌ Higher cost for always-on warehouse

### Why Star Schema over Normalized?
✅ Query performance, easier for analysts
❌ Some data redundancy

---

This architecture is designed for **production readiness**, **scalability**, and **maintainability** while demonstrating modern data engineering best practices.
