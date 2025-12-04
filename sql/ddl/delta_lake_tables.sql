-- Delta Lake Table Definitions
-- These are logical definitions; actual creation happens via PySpark code

-- ==============================================================================
-- RAW LAYER
-- ==============================================================================

-- Table: raw_transactions
-- Path: s3://aml-transaction-monitoring/data/raw_transactions/
-- Description: Raw transactions from Kafka, no transformations
-- Partitioned by: processing_date
-- Format: Delta Lake
/*
Schema:
- transaction_id: STRING (NOT NULL)
- customer_id: STRING (NOT NULL)
- transaction_type: STRING (NOT NULL)
- amount: DOUBLE (NOT NULL)
- currency: STRING (NOT NULL)
- source_country: STRING (NOT NULL)
- destination_country: STRING (NOT NULL)
- merchant_category: STRING
- timestamp: STRING (NOT NULL)
- channel: STRING (NOT NULL)
- device_id: STRING
- kafka_timestamp: TIMESTAMP
- kafka_partition: INTEGER
- kafka_offset: LONG
- processing_timestamp: TIMESTAMP
- processing_date: DATE (PARTITION KEY)

Properties:
- delta.autoOptimize.optimizeWrite = true
- delta.autoOptimize.autoCompact = true
- delta.deletedFileRetentionDuration = "interval 7 days"
*/

CREATE TABLE raw_transactions (
    transaction_id STRING NOT NULL,
    customer_id STRING NOT NULL,
    transaction_type STRING NOT NULL,
    amount DOUBLE NOT NULL,
    currency STRING NOT NULL,
    source_country STRING NOT NULL,
    destination_country STRING NOT NULL,
    merchant_category STRING,
    timestamp STRING NOT NULL,
    channel STRING NOT NULL,
    device_id STRING,
    kafka_timestamp TIMESTAMP,
    kafka_partition INT,
    kafka_offset LONG,
    processing_timestamp TIMESTAMP,
    processing_date DATE NOT NULL
)
USING DELTA
PARTITIONED BY (processing_date)
LOCATION 's3://aml-transaction-monitoring/data/raw_transactions/'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 7 days'
);

-- ==============================================================================
-- ENRICHED LAYER
-- ==============================================================================

-- Table: enriched_transactions
-- Path: s3://aml-transaction-monitoring/data/enriched_transactions/
-- Description: Transactions enriched with customer profiles and risk scores
-- Partitioned by: processing_date
-- Z-Ordered by: customer_id, transaction_timestamp
-- Format: Delta Lake
/*
Schema:
- All fields from raw_transactions
- Plus enrichment fields:
- transaction_timestamp: TIMESTAMP (converted from string)
- risk_score: DOUBLE (customer risk score)
- customer_segment: STRING
- account_age_days: INTEGER
- avg_transaction_amount: DOUBLE
- total_transactions_30d: INTEGER
- high_risk_country_count: INTEGER
- is_pep: BOOLEAN
- kyc_status: STRING
- amount_vs_avg_ratio: DOUBLE (derived)
- is_high_risk_country: BOOLEAN (derived)
- is_large_amount: BOOLEAN (derived)
- is_unusual_time: BOOLEAN (derived)
- rule_1_triggered: BOOLEAN
- rule_2_triggered: BOOLEAN
- rule_3_triggered: BOOLEAN
- rule_4_triggered: BOOLEAN
- rule_5_triggered: BOOLEAN
- rule_6_triggered: BOOLEAN
- total_rules_triggered: INTEGER
- transaction_risk_score: DOUBLE
- is_suspicious: BOOLEAN
- alert_priority: STRING
*/

CREATE TABLE enriched_transactions (
    transaction_id STRING NOT NULL,
    customer_id STRING NOT NULL,
    transaction_type STRING NOT NULL,
    amount DOUBLE NOT NULL,
    currency STRING NOT NULL,
    source_country STRING NOT NULL,
    destination_country STRING NOT NULL,
    merchant_category STRING,
    channel STRING NOT NULL,
    device_id STRING,
    transaction_timestamp TIMESTAMP NOT NULL,
    processing_timestamp TIMESTAMP,
    processing_date DATE NOT NULL,
    
    -- Customer Profile Enrichment
    risk_score DOUBLE,
    customer_segment STRING,
    account_age_days INT,
    avg_transaction_amount DOUBLE,
    total_transactions_30d INT,
    high_risk_country_count INT,
    is_pep BOOLEAN,
    kyc_status STRING,
    
    -- Derived Risk Features
    amount_vs_avg_ratio DOUBLE,
    is_high_risk_country BOOLEAN,
    is_large_amount BOOLEAN,
    is_unusual_time BOOLEAN,
    
    -- Rule Triggers
    rule_1_triggered BOOLEAN,
    rule_2_triggered BOOLEAN,
    rule_3_triggered BOOLEAN,
    rule_4_triggered BOOLEAN,
    rule_5_triggered BOOLEAN,
    rule_6_triggered BOOLEAN,
    total_rules_triggered INT,
    
    -- Risk Assessment
    transaction_risk_score DOUBLE,
    is_suspicious BOOLEAN,
    alert_priority STRING,
    
    -- Metadata
    kafka_timestamp TIMESTAMP,
    kafka_partition INT,
    kafka_offset LONG
)
USING DELTA
PARTITIONED BY (processing_date)
LOCATION 's3://aml-transaction-monitoring/data/enriched_transactions/'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 7 days'
);

-- ==============================================================================
-- CUSTOMER PROFILES (SCD Type 2)
-- ==============================================================================

-- Table: customer_profiles
-- Path: s3://aml-transaction-monitoring/data/customer_profiles/
-- Description: Customer risk profiles with full history (SCD Type 2)
-- Partitioned by: profile_date
-- Z-Ordered by: customer_id, effective_date
-- Format: Delta Lake
/*
Schema:
- customer_id: STRING (NOT NULL, Business Key)
- customer_name: STRING
- customer_segment: STRING
- account_open_date: DATE
- account_age_days: INTEGER
- country_of_residence: STRING
- is_pep: BOOLEAN
- kyc_status: STRING
- 
- Transaction Metrics:
- total_transactions: LONG
- total_volume: DOUBLE
- avg_transaction_amount: DOUBLE
- max_transaction_amount: DOUBLE
- stddev_transaction_amount: DOUBLE
- total_transactions_30d: INTEGER
- total_volume_30d: DOUBLE
- total_transactions_90d: INTEGER
- 
- Risk Indicators:
- high_risk_country_count: INTEGER
- suspicious_transaction_count: INTEGER
- wire_transfer_count: INTEGER
- cash_deposit_count: INTEGER
- 
- Behavioral Patterns:
- avg_hours_between_transactions: DOUBLE
- weekend_transactions: INTEGER
- night_transactions: INTEGER
- 
- Risk Score:
- risk_score: DOUBLE (0-1)
- risk_category: STRING (LOW/MEDIUM/HIGH)
- 
- SCD Type 2 Fields:
- effective_date: TIMESTAMP (NOT NULL)
- end_date: TIMESTAMP
- is_current: BOOLEAN (NOT NULL)
- profile_date: DATE (PARTITION KEY)
*/

CREATE TABLE customer_profiles (
    customer_id STRING NOT NULL,
    customer_name STRING,
    customer_segment STRING,
    account_open_date DATE,
    account_age_days INT,
    country_of_residence STRING,
    is_pep BOOLEAN,
    kyc_status STRING,
    
    -- Transaction Metrics
    total_transactions LONG,
    total_volume DOUBLE,
    avg_transaction_amount DOUBLE,
    max_transaction_amount DOUBLE,
    stddev_transaction_amount DOUBLE,
    total_transactions_30d INT,
    total_volume_30d DOUBLE,
    avg_amount_30d DOUBLE,
    total_transactions_90d INT,
    total_volume_90d DOUBLE,
    
    -- Transaction Type Distribution
    wire_transfer_count INT,
    cash_deposit_count INT,
    online_transfer_count INT,
    
    -- Risk Indicators
    high_risk_country_count INT,
    large_amount_count INT,
    unusual_time_count INT,
    suspicious_transaction_count INT,
    unique_source_countries INT,
    unique_destination_countries INT,
    
    -- Channel Distribution
    online_channel_count INT,
    branch_channel_count INT,
    atm_channel_count INT,
    
    -- Behavioral Patterns
    avg_hours_between_transactions DOUBLE,
    stddev_hours_between_transactions DOUBLE,
    weekend_transactions INT,
    weekday_transactions INT,
    morning_transactions INT,
    afternoon_transactions INT,
    evening_transactions INT,
    night_transactions INT,
    
    -- Risk Assessment
    volume_risk DOUBLE,
    frequency_risk DOUBLE,
    high_risk_country_risk DOUBLE,
    suspicious_activity_risk DOUBLE,
    unusual_behavior_risk DOUBLE,
    pep_risk DOUBLE,
    cash_risk DOUBLE,
    risk_score DOUBLE NOT NULL,
    risk_category STRING NOT NULL,
    
    -- Latest Activity
    last_transaction_date TIMESTAMP,
    
    -- SCD Type 2 Fields
    effective_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    is_current BOOLEAN NOT NULL,
    profile_date DATE NOT NULL,
    profile_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (profile_date)
LOCATION 's3://aml-transaction-monitoring/data/customer_profiles/'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 30 days'
);

-- ==============================================================================
-- ALERTS
-- ==============================================================================

-- Table: alerts
-- Path: s3://aml-transaction-monitoring/data/alerts/
-- Description: Suspicious transaction alerts
-- Partitioned by: processing_date (derived from alert_generated_at)
-- Z-Ordered by: customer_id, alert_generated_at
-- Format: Delta Lake

CREATE TABLE alerts (
    transaction_id STRING NOT NULL,
    customer_id STRING NOT NULL,
    transaction_type STRING NOT NULL,
    amount DOUBLE NOT NULL,
    currency STRING NOT NULL,
    source_country STRING NOT NULL,
    destination_country STRING NOT NULL,
    transaction_timestamp TIMESTAMP NOT NULL,
    
    -- Risk Metrics
    transaction_risk_score DOUBLE NOT NULL,
    alert_priority STRING NOT NULL,
    total_rules_triggered INT NOT NULL,
    
    -- Rule Details
    rule_1_triggered BOOLEAN,
    rule_2_triggered BOOLEAN,
    rule_3_triggered BOOLEAN,
    rule_4_triggered BOOLEAN,
    rule_5_triggered BOOLEAN,
    rule_6_triggered BOOLEAN,
    
    -- Alert Management
    alert_generated_at TIMESTAMP NOT NULL,
    alert_status STRING NOT NULL,
    reviewed_by STRING,
    reviewed_at TIMESTAMP,
    processing_date DATE NOT NULL
)
USING DELTA
PARTITIONED BY (processing_date)
LOCATION 's3://aml-transaction-monitoring/data/alerts/'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 90 days'
);

-- ==============================================================================
-- REFERENCE DATA
-- ==============================================================================

-- Table: customer_master
-- Path: s3://aml-transaction-monitoring/data/customer_master/
-- Description: Customer master data (SCD Type 2)
-- Format: Delta Lake

CREATE TABLE customer_master (
    customer_id STRING NOT NULL,
    customer_name STRING NOT NULL,
    account_open_date DATE NOT NULL,
    customer_segment STRING,
    country_of_residence STRING,
    is_pep BOOLEAN DEFAULT FALSE,
    kyc_status STRING DEFAULT 'PENDING',
    
    -- SCD Type 2
    effective_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    is_current BOOLEAN NOT NULL,
    
    -- Metadata
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
LOCATION 's3://aml-transaction-monitoring/data/customer_master/'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 180 days'
);

-- ==============================================================================
-- QUALITY METRICS
-- ==============================================================================

-- Table: data_quality_metrics
-- Path: s3://aml-transaction-monitoring/data/quality_metrics/
-- Description: Data quality check results
-- Partitioned by: check_date
-- Format: Parquet (not Delta, as it's append-only logs)

/*
Schema:
- check_date: DATE
- check_timestamp: TIMESTAMP
- table_name: STRING
- check_type: STRING (COMPLETENESS, VALIDITY, CONSISTENCY, etc.)
- check_name: STRING
- check_result: STRING
- passed: BOOLEAN
- metric_value: DOUBLE
- threshold_value: DOUBLE
*/

-- ==============================================================================
-- MAINTENANCE COMMANDS
-- ==============================================================================

-- Optimize tables (run weekly)
/*
OPTIMIZE raw_transactions ZORDER BY (customer_id);
OPTIMIZE enriched_transactions ZORDER BY (customer_id, transaction_timestamp);
OPTIMIZE customer_profiles ZORDER BY (customer_id, effective_date);
OPTIMIZE alerts ZORDER BY (customer_id, alert_generated_at);
*/

-- Vacuum old versions (run weekly, retain 7 days)
/*
VACUUM raw_transactions RETAIN 168 HOURS;
VACUUM enriched_transactions RETAIN 168 HOURS;
VACUUM customer_profiles RETAIN 720 HOURS; -- 30 days
VACUUM alerts RETAIN 2160 HOURS; -- 90 days
*/

-- ==============================================================================
-- USEFUL QUERIES
-- ==============================================================================

-- Count records by partition
/*
SELECT 
    processing_date, 
    COUNT(*) as record_count,
    COUNT(DISTINCT customer_id) as unique_customers
FROM enriched_transactions
GROUP BY processing_date
ORDER BY processing_date DESC;
*/

-- Check current customer profiles
/*
SELECT 
    customer_id,
    risk_category,
    risk_score,
    total_transactions_30d,
    suspicious_transaction_count
FROM customer_profiles
WHERE is_current = TRUE
ORDER BY risk_score DESC
LIMIT 100;
*/

-- Time travel query (read data from 7 days ago)
/*
SELECT * FROM enriched_transactions
VERSION AS OF 7;

SELECT * FROM enriched_transactions
TIMESTAMP AS OF '2024-01-01 00:00:00';
*/
