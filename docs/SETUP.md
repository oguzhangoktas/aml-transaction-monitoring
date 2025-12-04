# Setup Guide

This guide walks you through setting up the AML Transaction Monitoring pipeline both locally (for development) and on AWS (for production).

## Prerequisites

### Required Software
- **Python**: 3.9 or higher
- **Java**: JDK 11 (for Spark)
- **Docker**: 20.10+ and Docker Compose
- **AWS CLI**: 2.x
- **Terraform**: 1.0+
- **Git**: 2.x

### AWS Account Requirements
- Active AWS account with billing enabled
- IAM user with administrator access (for initial setup)
- AWS CLI configured with credentials

### Recommended Tools
- **VS Code** with Python extension
- **DBeaver** or **DataGrip** for database management
- **Postman** for API testing

---

## Part 1: Local Development Setup

### Step 1: Clone Repository

```bash
git clone https://github.com/yourusername/aml-transaction-monitoring.git
cd aml-transaction-monitoring
```

### Step 2: Create Python Virtual Environment

```bash
# Create virtual environment
python3.9 -m venv venv

# Activate (Linux/Mac)
source venv/bin/activate

# Activate (Windows)
.\venv\Scripts\activate

# Upgrade pip
pip install --upgrade pip
```

### Step 3: Install Python Dependencies

```bash
# Install all dependencies
pip install -r requirements.txt

# Verify installation
python -c "import pyspark; print(pyspark.__version__)"
python -c "import delta; print('Delta Lake installed')"
```

**requirements.txt** includes:
```
pyspark==3.4.1
delta-spark==2.4.0
kafka-python==2.0.2
boto3==1.28.85
apache-airflow==2.7.3
great-expectations==0.18.3
pytest==7.4.3
pytest-cov==4.1.0
black==23.11.0
pylint==3.0.2
```

### Step 4: Set Up Local Kafka + Spark

```bash
cd infrastructure/docker

# Start services
docker-compose up -d

# Verify services are running
docker-compose ps

# Expected output:
# kafka          running    0.0.0.0:9092->9092/tcp
# zookeeper      running    0.0.0.0:2181->2181/tcp
# spark-master   running    0.0.0.0:8080->8080/tcp
# spark-worker   running    0.0.0.0:8081->8081/tcp
```

### Step 5: Create Kafka Topics

```bash
# Create transactions topic
docker exec -it kafka kafka-topics.sh \
  --create \
  --bootstrap-server localhost:9092 \
  --topic transactions-raw \
  --partitions 6 \
  --replication-factor 1

# Create alerts topic
docker exec -it kafka kafka-topics.sh \
  --create \
  --bootstrap-server localhost:9092 \
  --topic alerts-high-priority \
  --partitions 3 \
  --replication-factor 1

# List topics to verify
docker exec -it kafka kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

### Step 6: Set Up Local Delta Lake Storage

```bash
# Create local directories for Delta Lake
mkdir -p data/delta/bronze/transactions
mkdir -p data/delta/silver/transactions
mkdir -p data/delta/gold/alerts
mkdir -p data/delta/gold/customer_daily_summary

# Create checkpoints directory
mkdir -p data/checkpoints

# Set permissions
chmod -R 755 data/
```

### Step 7: Configure Environment Variables

Create `.env` file in project root:

```bash
# Copy template
cp .env.example .env

# Edit with your values
nano .env
```

**.env** contents:
```bash
# Environment
ENVIRONMENT=local

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_TRANSACTIONS=transactions-raw
KAFKA_TOPIC_ALERTS=alerts-high-priority

# Spark
SPARK_MASTER=local[*]
SPARK_APP_NAME=aml-transaction-monitoring

# Delta Lake
DELTA_LAKE_PATH=./data/delta
CHECKPOINT_PATH=./data/checkpoints

# AWS (for local testing with LocalStack)
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_DEFAULT_REGION=eu-central-1

# Database (local PostgreSQL for Airflow)
AIRFLOW_DB_HOST=localhost
AIRFLOW_DB_PORT=5432
AIRFLOW_DB_NAME=airflow
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=airflow

# Logging
LOG_LEVEL=INFO
```

### Step 8: Initialize Sample Data

```bash
# Generate sample customer data
python scripts/generate_sample_customers.py --count 1000

# Generate sample exchange rates
python scripts/generate_exchange_rates.py --days 90

# Verify data files created
ls -lh data/sample/
```

### Step 9: Run Data Generator (Kafka Producer)

Open a new terminal:

```bash
# Activate virtual environment
source venv/bin/activate

# Start producing transactions
python src/data_generator/transaction_producer.py \
  --rate 100 \
  --duration 3600

# Expected output:
# [INFO] Connected to Kafka: localhost:9092
# [INFO] Producing transactions at 100 TPS
# [INFO] Sent 1000 transactions...
# [INFO] Sent 2000 transactions...
```

### Step 10: Run Streaming Processor

Open another terminal:

```bash
# Activate virtual environment
source venv/bin/activate

# Run Spark Streaming job locally
spark-submit \
  --master local[4] \
  --packages io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.databricks.delta.retentionDurationCheck.enabled=false" \
  src/glue_jobs/streaming_transaction_processor.py

# Expected output:
# [INFO] Starting AML Transaction Processor
# [INFO] Reading from Kafka: transactions-raw
# [INFO] Writing to Delta Lake: ./data/delta/bronze/transactions
# [INFO] Batch 0: Processed 500 records
# [INFO] Batch 1: Processed 500 records
```

### Step 11: Verify Data in Delta Lake

```bash
# Start PySpark shell
pyspark \
  --packages io.delta:delta-core_2.12:2.4.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

# In PySpark shell:
>>> from delta.tables import *
>>> 
>>> # Read Bronze transactions
>>> bronze_df = spark.read.format("delta").load("./data/delta/bronze/transactions")
>>> print(f"Bronze count: {bronze_df.count()}")
>>> bronze_df.show(5)
>>> 
>>> # Read Silver transactions
>>> silver_df = spark.read.format("delta").load("./data/delta/silver/transactions")
>>> print(f"Silver count: {silver_df.count()}")
>>> silver_df.show(5)
>>> 
>>> # Read Gold alerts
>>> alerts_df = spark.read.format("delta").load("./data/delta/gold/alerts")
>>> print(f"Alerts generated: {alerts_df.count()}")
>>> alerts_df.show(5)
```

### Step 12: Set Up Local Airflow (Optional)

```bash
# Initialize Airflow database
cd airflow
export AIRFLOW_HOME=$(pwd)
airflow db init

# Create admin user
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin

# Start Airflow webserver (terminal 1)
airflow webserver --port 8080

# Start Airflow scheduler (terminal 2)
airflow scheduler

# Access UI: http://localhost:8080
# Login: admin / admin
```

### Step 13: Run Tests

```bash
# Run unit tests
pytest tests/unit/ -v

# Run with coverage
pytest tests/unit/ --cov=src --cov-report=html

# Run integration tests (requires Docker services)
pytest tests/integration/ -v

# View coverage report
open htmlcov/index.html
```

---

## Part 2: AWS Production Deployment

### Step 1: Configure AWS CLI

```bash
# Configure credentials
aws configure

# Verify access
aws sts get-caller-identity

# Expected output:
# {
#     "UserId": "AIDAQ...",
#     "Account": "123456789012",
#     "Arn": "arn:aws:iam::123456789012:user/your-username"
# }
```

### Step 2: Create S3 Buckets

```bash
# Set variables
export AWS_REGION=eu-central-1
export PROJECT_NAME=aml-transaction-monitoring
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Create buckets
aws s3 mb s3://${PROJECT_NAME}-data-lake-${ACCOUNT_ID} --region ${AWS_REGION}
aws s3 mb s3://${PROJECT_NAME}-glue-scripts-${ACCOUNT_ID} --region ${AWS_REGION}
aws s3 mb s3://${PROJECT_NAME}-glue-temp-${ACCOUNT_ID} --region ${AWS_REGION}

# Enable versioning on data lake bucket
aws s3api put-bucket-versioning \
  --bucket ${PROJECT_NAME}-data-lake-${ACCOUNT_ID} \
  --versioning-configuration Status=Enabled

# Enable encryption
aws s3api put-bucket-encryption \
  --bucket ${PROJECT_NAME}-data-lake-${ACCOUNT_ID} \
  --server-side-encryption-configuration \
  '{"Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]}'
```

### Step 3: Deploy Infrastructure with Terraform

```bash
cd infrastructure/terraform

# Initialize Terraform
terraform init

# Review plan
terraform plan \
  -var="project_name=${PROJECT_NAME}" \
  -var="aws_region=${AWS_REGION}" \
  -var="environment=production"

# Apply (creates ~20 resources)
terraform apply \
  -var="project_name=${PROJECT_NAME}" \
  -var="aws_region=${AWS_REGION}" \
  -var="environment=production"

# Expected resources:
# - AWS MSK Cluster (3 brokers)
# - S3 Buckets (data lake, scripts, temp)
# - Redshift Cluster (2 nodes)
# - Glue Database & Tables
# - IAM Roles & Policies
# - CloudWatch Log Groups
# - Security Groups
# - VPC & Subnets

# Save outputs
terraform output -json > outputs.json
```

### Step 4: Upload Glue Job Scripts

```bash
# Package dependencies
cd ../..
mkdir -p dist
pip install --target ./dist -r requirements.txt

# Create deployment package
cd dist
zip -r ../glue-dependencies.zip .
cd ..

# Upload to S3
aws s3 cp glue-dependencies.zip s3://${PROJECT_NAME}-glue-scripts-${ACCOUNT_ID}/dependencies/

# Upload Glue job scripts
aws s3 sync src/glue_jobs/ s3://${PROJECT_NAME}-glue-scripts-${ACCOUNT_ID}/jobs/
```

### Step 5: Create Glue Jobs

```bash
# Create streaming job
aws glue create-job \
  --name aml-streaming-processor \
  --role $(terraform output -raw glue_role_arn) \
  --command '{"Name": "gluestreaming", "ScriptLocation": "s3://'${PROJECT_NAME}'-glue-scripts-'${ACCOUNT_ID}'/jobs/streaming_transaction_processor.py"}' \
  --default-arguments '{
    "--enable-metrics": "true",
    "--enable-spark-ui": "true",
    "--spark-event-logs-path": "s3://'${PROJECT_NAME}'-glue-temp-'${ACCOUNT_ID}'/spark-logs/",
    "--enable-job-insights": "true",
    "--job-language": "python",
    "--TempDir": "s3://'${PROJECT_NAME}'-glue-temp-'${ACCOUNT_ID}'/temp/",
    "--extra-py-files": "s3://'${PROJECT_NAME}'-glue-scripts-'${ACCOUNT_ID}'/dependencies/glue-dependencies.zip"
  }' \
  --glue-version "4.0" \
  --worker-type "G.1X" \
  --number-of-workers 10 \
  --timeout 2880

# Create batch profile job
aws glue create-job \
  --name aml-batch-profile-aggregator \
  --role $(terraform output -raw glue_role_arn) \
  --command '{"Name": "glueetl", "ScriptLocation": "s3://'${PROJECT_NAME}'-glue-scripts-'${ACCOUNT_ID}'/jobs/batch_profile_aggregator.py"}' \
  --default-arguments '{
    "--enable-metrics": "true",
    "--TempDir": "s3://'${PROJECT_NAME}'-glue-temp-'${ACCOUNT_ID}'/temp/"
  }' \
  --glue-version "4.0" \
  --worker-type "G.1X" \
  --number-of-workers 5 \
  --timeout 60

# Verify jobs created
aws glue list-jobs --query 'JobNames' --output table
```

### Step 6: Create Kafka Topics in MSK

```bash
# Get MSK bootstrap servers
MSK_BOOTSTRAP=$(aws kafka describe-cluster \
  --cluster-arn $(terraform output -raw msk_cluster_arn) \
  --query 'ClusterInfo.ZookeeperConnectString' \
  --output text)

# Connect to MSK from bastion host (or use Kafka client)
# Note: You'll need to set up a bastion host or use AWS Cloud9

# Create topics via AWS Console or using Kafka client:
# 1. Go to MSK Console
# 2. Select cluster
# 3. View client information
# 4. Use provided connection string

# Alternative: Use MSK configuration to auto-create topics
```

### Step 7: Initialize Redshift Schema

```bash
# Get Redshift endpoint
REDSHIFT_ENDPOINT=$(terraform output -raw redshift_endpoint)
REDSHIFT_PORT=$(terraform output -raw redshift_port)
REDSHIFT_DB=$(terraform output -raw redshift_database)

# Connect using psql (or DBeaver/DataGrip)
psql -h $REDSHIFT_ENDPOINT -p $REDSHIFT_PORT -U admin -d $REDSHIFT_DB

# Run DDL scripts
\i sql/ddl/redshift_star_schema.sql

# Verify tables created
\dt

# Expected output:
# public | dim_customer
# public | dim_rule
# public | dim_date
# public | dim_account
# public | fact_alerts
```

### Step 8: Set Up Airflow on AWS (MWAA)

```bash
# Create MWAA environment (via Terraform or Console)
# Note: MWAA is expensive (~$300/month), consider self-hosted Airflow on EC2

# Upload DAGs to S3
aws s3 sync airflow/dags/ s3://${PROJECT_NAME}-airflow-${ACCOUNT_ID}/dags/

# Upload requirements.txt
aws s3 cp requirements.txt s3://${PROJECT_NAME}-airflow-${ACCOUNT_ID}/requirements.txt

# MWAA will auto-install dependencies from requirements.txt
```

### Step 9: Start Glue Streaming Job

```bash
# Start the streaming job
aws glue start-job-run --job-name aml-streaming-processor

# Monitor job
JOB_RUN_ID=$(aws glue get-job-runs \
  --job-name aml-streaming-processor \
  --max-results 1 \
  --query 'JobRuns[0].Id' \
  --output text)

# Check status
aws glue get-job-run \
  --job-name aml-streaming-processor \
  --run-id $JOB_RUN_ID \
  --query 'JobRun.JobRunState' \
  --output text

# View logs
aws logs tail /aws-glue/jobs/output \
  --follow \
  --filter-pattern "aml-streaming-processor"
```

### Step 10: Verify Data Pipeline

```bash
# Check S3 for Delta Lake files
aws s3 ls s3://${PROJECT_NAME}-data-lake-${ACCOUNT_ID}/delta/bronze/transactions/ --recursive

# Check Redshift for loaded data
psql -h $REDSHIFT_ENDPOINT -p $REDSHIFT_PORT -U admin -d $REDSHIFT_DB \
  -c "SELECT COUNT(*) FROM fact_alerts;"

# Check CloudWatch metrics
aws cloudwatch get-metric-statistics \
  --namespace AWS/Glue \
  --metric-name glue.driver.aggregate.numRecordsRead \
  --dimensions Name=JobName,Value=aml-streaming-processor \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 300 \
  --statistics Sum
```

---

## Part 3: Production Operations

### Monitoring Setup

1. **CloudWatch Dashboard**
```bash
# Create custom dashboard
aws cloudwatch put-dashboard \
  --dashboard-name AML-Pipeline-Monitoring \
  --dashboard-body file://infrastructure/cloudwatch/dashboard.json
```

2. **SNS Alerts**
```bash
# Create SNS topic
aws sns create-topic --name aml-pipeline-alerts

# Subscribe email
aws sns subscribe \
  --topic-arn arn:aws:sns:${AWS_REGION}:${ACCOUNT_ID}:aml-pipeline-alerts \
  --protocol email \
  --notification-endpoint your-email@example.com
```

3. **CloudWatch Alarms**
```bash
# Glue job failure alarm
aws cloudwatch put-metric-alarm \
  --alarm-name aml-glue-job-failure \
  --alarm-description "Alert on Glue job failures" \
  --metric-name Failures \
  --namespace AWS/Glue \
  --statistic Sum \
  --period 300 \
  --threshold 1 \
  --comparison-operator GreaterThanThreshold \
  --evaluation-periods 1 \
  --alarm-actions arn:aws:sns:${AWS_REGION}:${ACCOUNT_ID}:aml-pipeline-alerts
```

### Backup & Disaster Recovery

```bash
# Enable automated Redshift snapshots
aws redshift modify-cluster \
  --cluster-identifier aml-redshift-cluster \
  --automated-snapshot-retention-period 7

# Create manual snapshot
aws redshift create-cluster-snapshot \
  --snapshot-identifier aml-redshift-$(date +%Y%m%d) \
  --cluster-identifier aml-redshift-cluster

# S3 versioning already enabled in Step 2
```

### Cost Monitoring

```bash
# Create cost budget
aws budgets create-budget \
  --account-id $ACCOUNT_ID \
  --budget file://infrastructure/budgets/monthly-budget.json \
  --notifications-with-subscribers file://infrastructure/budgets/notifications.json

# View current costs
aws ce get-cost-and-usage \
  --time-period Start=$(date -d '1 month ago' +%Y-%m-%d),End=$(date +%Y-%m-%d) \
  --granularity MONTHLY \
  --metrics BlendedCost \
  --group-by Type=SERVICE
```

---

## Troubleshooting

### Issue: Kafka connection refused

**Solution**:
```bash
# Check if Kafka is running
docker-compose ps kafka

# Check Kafka logs
docker-compose logs kafka

# Restart Kafka
docker-compose restart kafka
```

### Issue: Spark job fails with memory error

**Solution**:
```bash
# Increase driver/executor memory
spark-submit \
  --driver-memory 4g \
  --executor-memory 4g \
  --conf "spark.sql.shuffle.partitions=200" \
  src/glue_jobs/streaming_transaction_processor.py
```

### Issue: Delta Lake table not found

**Solution**:
```python
# Check if path exists
from delta.tables import DeltaTable
if DeltaTable.isDeltaTable(spark, "./data/delta/bronze/transactions"):
    print("Table exists")
else:
    print("Table not found - check path and run data generator first")
```

### Issue: AWS Glue job fails with permission error

**Solution**:
```bash
# Verify IAM role has required permissions
aws iam get-role-policy \
  --role-name AMLGlueRole \
  --policy-name AMLGluePolicy

# Add missing permissions if needed
aws iam put-role-policy \
  --role-name AMLGlueRole \
  --policy-name AMLGluePolicy \
  --policy-document file://infrastructure/iam/glue-policy.json
```

---

## Next Steps

1. **Run data quality tests**: `pytest tests/data_quality/`
2. **Set up CI/CD**: See `.github/workflows/ci.yml`
3. **Configure monitoring**: Set up Grafana dashboards
4. **Load historical data**: Backfill script in `scripts/backfill_historical.py`
5. **Optimize performance**: Run `ANALYZE` and `VACUUM` on Redshift tables

---

## Support

For issues or questions:
1. Check [Architecture Documentation](ARCHITECTURE.md)
2. Review [Data Model](DATA_MODEL.md)
3. Open GitHub issue
4. Contact: oguzhangoktas@example.com

---

**Setup complete! 🎉**

Your AML Transaction Monitoring pipeline is now ready for development or production use.
