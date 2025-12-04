-- Redshift Data Warehouse Schema for AML Transaction Monitoring
-- Star Schema Design for Analytics and Reporting

-- ==============================================================================
-- DIMENSION TABLES
-- ==============================================================================

-- Dimension: Customer
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_sk BIGINT IDENTITY(1,1) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(200),
    customer_segment VARCHAR(50),
    country_of_residence VARCHAR(3),
    is_pep BOOLEAN DEFAULT FALSE,
    kyc_status VARCHAR(20),
    account_open_date DATE,
    account_age_days INTEGER,
    risk_category VARCHAR(20),
    risk_score DECIMAL(5,4),
    effective_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT GETDATE(),
    updated_at TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE KEY
DISTKEY (customer_id)
SORTKEY (customer_id, effective_date);

-- Dimension: Date
CREATE TABLE IF NOT EXISTS dim_date (
    date_sk INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_week INTEGER,
    day_name VARCHAR(10),
    day_of_month INTEGER,
    day_of_year INTEGER,
    week_of_year INTEGER,
    month_number INTEGER,
    month_name VARCHAR(10),
    quarter INTEGER,
    year INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER
)
DISTSTYLE ALL
SORTKEY (full_date);

-- Dimension: Transaction Type
CREATE TABLE IF NOT EXISTS dim_transaction_type (
    transaction_type_sk INTEGER IDENTITY(1,1) PRIMARY KEY,
    transaction_type_code VARCHAR(50) NOT NULL UNIQUE,
    transaction_type_name VARCHAR(100),
    transaction_category VARCHAR(50),
    base_risk_level DECIMAL(3,2),
    description VARCHAR(500),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE ALL
SORTKEY (transaction_type_code);

-- Dimension: Geography
CREATE TABLE IF NOT EXISTS dim_geography (
    geography_sk INTEGER IDENTITY(1,1) PRIMARY KEY,
    country_code VARCHAR(3) NOT NULL UNIQUE,
    country_name VARCHAR(100),
    region VARCHAR(50),
    risk_rating VARCHAR(20),
    risk_score DECIMAL(3,2),
    is_high_risk BOOLEAN DEFAULT FALSE,
    is_sanctioned BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT GETDATE(),
    updated_at TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE ALL
SORTKEY (country_code);

-- Dimension: Alert Status
CREATE TABLE IF NOT EXISTS dim_alert_status (
    alert_status_sk INTEGER IDENTITY(1,1) PRIMARY KEY,
    status_code VARCHAR(20) NOT NULL UNIQUE,
    status_name VARCHAR(50),
    status_description VARCHAR(200),
    is_open BOOLEAN,
    sort_order INTEGER,
    created_at TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE ALL
SORTKEY (status_code);

-- ==============================================================================
-- FACT TABLES
-- ==============================================================================

-- Fact: Transactions
CREATE TABLE IF NOT EXISTS fact_transaction (
    transaction_sk BIGINT IDENTITY(1,1) PRIMARY KEY,
    transaction_id VARCHAR(100) NOT NULL UNIQUE,
    customer_sk BIGINT NOT NULL,
    transaction_type_sk INTEGER NOT NULL,
    source_geography_sk INTEGER NOT NULL,
    dest_geography_sk INTEGER NOT NULL,
    transaction_date_sk INTEGER NOT NULL,
    
    -- Transaction Details
    transaction_timestamp TIMESTAMP NOT NULL,
    processing_timestamp TIMESTAMP NOT NULL,
    amount DECIMAL(18,2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    channel VARCHAR(20),
    device_id VARCHAR(50),
    merchant_category VARCHAR(50),
    
    -- Risk Metrics
    transaction_risk_score DECIMAL(5,4),
    is_suspicious BOOLEAN DEFAULT FALSE,
    is_high_risk_country BOOLEAN DEFAULT FALSE,
    is_large_amount BOOLEAN DEFAULT FALSE,
    is_unusual_time BOOLEAN DEFAULT FALSE,
    amount_vs_avg_ratio DECIMAL(10,2),
    
    -- Rule Triggers
    rule_1_triggered BOOLEAN DEFAULT FALSE,
    rule_2_triggered BOOLEAN DEFAULT FALSE,
    rule_3_triggered BOOLEAN DEFAULT FALSE,
    rule_4_triggered BOOLEAN DEFAULT FALSE,
    rule_5_triggered BOOLEAN DEFAULT FALSE,
    rule_6_triggered BOOLEAN DEFAULT FALSE,
    total_rules_triggered INTEGER,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT GETDATE(),
    
    FOREIGN KEY (customer_sk) REFERENCES dim_customer(customer_sk),
    FOREIGN KEY (transaction_type_sk) REFERENCES dim_transaction_type(transaction_type_sk),
    FOREIGN KEY (source_geography_sk) REFERENCES dim_geography(geography_sk),
    FOREIGN KEY (dest_geography_sk) REFERENCES dim_geography(geography_sk),
    FOREIGN KEY (transaction_date_sk) REFERENCES dim_date(date_sk)
)
DISTSTYLE KEY
DISTKEY (customer_sk)
SORTKEY (transaction_date_sk, transaction_timestamp);

-- Fact: Alerts
CREATE TABLE IF NOT EXISTS fact_alert (
    alert_sk BIGINT IDENTITY(1,1) PRIMARY KEY,
    alert_id VARCHAR(100) NOT NULL UNIQUE,
    transaction_sk BIGINT NOT NULL,
    customer_sk BIGINT NOT NULL,
    alert_status_sk INTEGER NOT NULL,
    alert_date_sk INTEGER NOT NULL,
    
    -- Alert Details
    alert_generated_at TIMESTAMP NOT NULL,
    alert_priority VARCHAR(20),
    transaction_risk_score DECIMAL(5,4),
    total_rules_triggered INTEGER,
    
    -- Resolution Details
    reviewed_by VARCHAR(100),
    reviewed_at TIMESTAMP,
    resolution_notes VARCHAR(1000),
    case_id VARCHAR(100),
    is_str_filed BOOLEAN DEFAULT FALSE,
    str_filed_date TIMESTAMP,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT GETDATE(),
    updated_at TIMESTAMP DEFAULT GETDATE(),
    
    FOREIGN KEY (transaction_sk) REFERENCES fact_transaction(transaction_sk),
    FOREIGN KEY (customer_sk) REFERENCES dim_customer(customer_sk),
    FOREIGN KEY (alert_status_sk) REFERENCES dim_alert_status(alert_status_sk),
    FOREIGN KEY (alert_date_sk) REFERENCES dim_date(date_sk)
)
DISTSTYLE KEY
DISTKEY (customer_sk)
SORTKEY (alert_date_sk, alert_generated_at);

-- Fact: Daily Customer Metrics
CREATE TABLE IF NOT EXISTS fact_customer_daily_metrics (
    customer_daily_sk BIGINT IDENTITY(1,1) PRIMARY KEY,
    customer_sk BIGINT NOT NULL,
    metric_date_sk INTEGER NOT NULL,
    
    -- Daily Transaction Metrics
    daily_transaction_count INTEGER,
    daily_transaction_volume DECIMAL(18,2),
    daily_avg_transaction_amount DECIMAL(18,2),
    daily_max_transaction_amount DECIMAL(18,2),
    
    -- Risk Indicators
    daily_suspicious_count INTEGER,
    daily_high_risk_country_count INTEGER,
    daily_large_amount_count INTEGER,
    daily_unusual_time_count INTEGER,
    
    -- Channel Distribution
    online_transaction_count INTEGER,
    branch_transaction_count INTEGER,
    atm_transaction_count INTEGER,
    
    -- Current Risk Profile
    current_risk_score DECIMAL(5,4),
    current_risk_category VARCHAR(20),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT GETDATE(),
    
    FOREIGN KEY (customer_sk) REFERENCES dim_customer(customer_sk),
    FOREIGN KEY (metric_date_sk) REFERENCES dim_date(date_sk),
    UNIQUE (customer_sk, metric_date_sk)
)
DISTSTYLE KEY
DISTKEY (customer_sk)
SORTKEY (metric_date_sk, customer_sk);

-- ==============================================================================
-- AGGREGATE TABLES (For Performance)
-- ==============================================================================

-- Monthly Transaction Summary
CREATE TABLE IF NOT EXISTS agg_monthly_transaction_summary (
    summary_sk BIGINT IDENTITY(1,1) PRIMARY KEY,
    year_month VARCHAR(7) NOT NULL,
    transaction_type_sk INTEGER NOT NULL,
    geography_sk INTEGER NOT NULL,
    
    transaction_count BIGINT,
    total_volume DECIMAL(18,2),
    avg_amount DECIMAL(18,2),
    suspicious_count BIGINT,
    suspicious_volume DECIMAL(18,2),
    unique_customers INTEGER,
    
    created_at TIMESTAMP DEFAULT GETDATE(),
    
    FOREIGN KEY (transaction_type_sk) REFERENCES dim_transaction_type(transaction_type_sk),
    FOREIGN KEY (geography_sk) REFERENCES dim_geography(geography_sk),
    UNIQUE (year_month, transaction_type_sk, geography_sk)
)
DISTSTYLE ALL
SORTKEY (year_month);

-- Customer Risk Score History
CREATE TABLE IF NOT EXISTS agg_customer_risk_history (
    risk_history_sk BIGINT IDENTITY(1,1) PRIMARY KEY,
    customer_sk BIGINT NOT NULL,
    snapshot_date DATE NOT NULL,
    
    risk_score DECIMAL(5,4),
    risk_category VARCHAR(20),
    total_transactions_30d INTEGER,
    total_volume_30d DECIMAL(18,2),
    suspicious_count_30d INTEGER,
    high_risk_country_count_30d INTEGER,
    
    created_at TIMESTAMP DEFAULT GETDATE(),
    
    FOREIGN KEY (customer_sk) REFERENCES dim_customer(customer_sk),
    UNIQUE (customer_sk, snapshot_date)
)
DISTSTYLE KEY
DISTKEY (customer_sk)
SORTKEY (customer_sk, snapshot_date);

-- ==============================================================================
-- INDEXES
-- ==============================================================================

-- Additional indexes for query performance
CREATE INDEX idx_transaction_customer ON fact_transaction(customer_sk, transaction_timestamp);
CREATE INDEX idx_transaction_suspicious ON fact_transaction(is_suspicious, transaction_date_sk);
CREATE INDEX idx_alert_status ON fact_alert(alert_status_sk, alert_date_sk);
CREATE INDEX idx_alert_customer ON fact_alert(customer_sk, alert_generated_at);

-- ==============================================================================
-- VIEWS
-- ==============================================================================

-- View: Current High-Risk Customers
CREATE OR REPLACE VIEW vw_high_risk_customers AS
SELECT 
    c.customer_id,
    c.customer_name,
    c.risk_score,
    c.risk_category,
    c.customer_segment,
    c.country_of_residence,
    c.is_pep
FROM dim_customer c
WHERE c.is_current = TRUE
  AND c.risk_category = 'HIGH'
ORDER BY c.risk_score DESC;

-- View: Open Alerts Summary
CREATE OR REPLACE VIEW vw_open_alerts AS
SELECT 
    a.alert_id,
    c.customer_id,
    c.customer_name,
    a.alert_priority,
    a.transaction_risk_score,
    a.alert_generated_at,
    DATEDIFF(day, a.alert_generated_at, GETDATE()) as days_open,
    a.total_rules_triggered
FROM fact_alert a
JOIN dim_customer c ON a.customer_sk = c.customer_sk
JOIN dim_alert_status s ON a.alert_status_sk = s.alert_status_sk
WHERE s.is_open = TRUE
  AND c.is_current = TRUE
ORDER BY a.alert_priority, a.alert_generated_at;

-- View: Daily Transaction Volume by Country
CREATE OR REPLACE VIEW vw_daily_volume_by_country AS
SELECT 
    d.full_date,
    g.country_name,
    g.risk_rating,
    COUNT(t.transaction_id) as transaction_count,
    SUM(t.amount) as total_volume,
    AVG(t.amount) as avg_amount,
    SUM(CASE WHEN t.is_suspicious THEN 1 ELSE 0 END) as suspicious_count
FROM fact_transaction t
JOIN dim_date d ON t.transaction_date_sk = d.date_sk
JOIN dim_geography g ON t.dest_geography_sk = g.geography_sk
GROUP BY d.full_date, g.country_name, g.risk_rating;

-- ==============================================================================
-- GRANTS (Adjust based on your security requirements)
-- ==============================================================================

-- Grant read access to analytics role
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO analytics_role;

-- Grant write access to ETL role
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO etl_role;

-- ==============================================================================
-- MAINTENANCE QUERIES
-- ==============================================================================

-- Vacuum and analyze tables (run periodically)
-- VACUUM fact_transaction;
-- VACUUM fact_alert;
-- ANALYZE fact_transaction;
-- ANALYZE fact_alert;
