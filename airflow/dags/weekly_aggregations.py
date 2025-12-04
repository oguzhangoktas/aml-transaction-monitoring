"""
Airflow DAG: Weekly Aggregations and Maintenance
Performs weekly analytics aggregations and Delta Lake maintenance
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
import logging

logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['oguzhangoktas22@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=4),
}

# DAG definition
dag = DAG(
    'weekly_aggregations_and_maintenance',
    default_args=default_args,
    description='Weekly analytics aggregations and Delta Lake optimization',
    schedule_interval='0 3 * * 0',  # Run at 3 AM UTC every Sunday
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['aml', 'weekly', 'maintenance'],
    max_active_runs=1,
)

# Configuration
S3_BUCKET = 's3://aml-transaction-monitoring'
DELTA_TABLES = [
    'data/raw_transactions',
    'data/enriched_transactions',
    'data/customer_profiles',
    'data/alerts',
]


def optimize_delta_table(table_path: str):
    """Optimize Delta Lake table"""
    from pyspark.sql import SparkSession
    from delta.tables import DeltaTable
    
    spark = SparkSession.builder \
        .appName("DeltaOptimization") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    full_path = f"{S3_BUCKET}/{table_path}"
    logger.info(f"Optimizing Delta table: {full_path}")
    
    try:
        delta_table = DeltaTable.forPath(spark, full_path)
        
        # Run optimization with Z-ordering on key columns
        if 'customer_id' in delta_table.toDF().columns:
            delta_table.optimize().executeZOrderBy("customer_id", "processing_date")
        else:
            delta_table.optimize().executeCompaction()
        
        logger.info(f"Optimization completed for {table_path}")
        
    except Exception as e:
        logger.error(f"Error optimizing {table_path}: {str(e)}")
        raise
    
    finally:
        spark.stop()


def vacuum_delta_table(table_path: str, retention_hours: int = 168):
    """Vacuum Delta Lake table to remove old versions"""
    from pyspark.sql import SparkSession
    from delta.tables import DeltaTable
    
    spark = SparkSession.builder \
        .appName("DeltaVacuum") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .getOrCreate()
    
    full_path = f"{S3_BUCKET}/{table_path}"
    logger.info(f"Vacuuming Delta table: {full_path} (retention: {retention_hours}h)")
    
    try:
        delta_table = DeltaTable.forPath(spark, full_path)
        delta_table.vacuum(retention_hours)
        logger.info(f"Vacuum completed for {table_path}")
        
    except Exception as e:
        logger.error(f"Error vacuuming {table_path}: {str(e)}")
        raise
    
    finally:
        spark.stop()


def generate_weekly_analytics(**context):
    """Generate weekly analytics report"""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    
    execution_date = context['ds']
    logger.info(f"Generating weekly analytics for week ending {execution_date}")
    
    spark = SparkSession.builder \
        .appName("WeeklyAnalytics") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    try:
        # Calculate week start date
        week_start = datetime.strptime(execution_date, '%Y-%m-%d') - timedelta(days=7)
        
        # Read enriched transactions
        transactions_df = spark.read.format("delta") \
            .load(f"{S3_BUCKET}/data/enriched_transactions/") \
            .filter(
                (F.col("processing_date") >= week_start.date()) & 
                (F.col("processing_date") <= execution_date)
            )
        
        # Weekly transaction summary
        weekly_summary = transactions_df.groupBy("transaction_type").agg(
            F.count("transaction_id").alias("transaction_count"),
            F.sum("amount").alias("total_volume"),
            F.avg("amount").alias("avg_amount"),
            F.sum(F.when(F.col("is_suspicious"), 1).otherwise(0)).alias("suspicious_count")
        )
        
        # Write to S3
        output_path = f"{S3_BUCKET}/analytics/weekly_summary/week_ending={execution_date}"
        weekly_summary.write.format("parquet").mode("overwrite").save(output_path)
        
        logger.info(f"Weekly analytics written to {output_path}")
        
        # Customer risk trends
        customer_trends = transactions_df.groupBy("customer_id").agg(
            F.count("transaction_id").alias("weekly_transaction_count"),
            F.sum("amount").alias("weekly_volume"),
            F.avg("transaction_risk_score").alias("avg_risk_score"),
            F.max("transaction_risk_score").alias("max_risk_score")
        )
        
        trends_path = f"{S3_BUCKET}/analytics/customer_trends/week_ending={execution_date}"
        customer_trends.write.format("parquet").mode("overwrite").save(trends_path)
        
        logger.info(f"Customer trends written to {trends_path}")
        
    except Exception as e:
        logger.error(f"Error generating weekly analytics: {str(e)}")
        raise
    
    finally:
        spark.stop()


def check_table_health(**context):
    """Check Delta table health metrics"""
    from pyspark.sql import SparkSession
    from delta.tables import DeltaTable
    
    spark = SparkSession.builder \
        .appName("TableHealth") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    health_report = []
    
    for table_path in DELTA_TABLES:
        full_path = f"{S3_BUCKET}/{table_path}"
        
        try:
            delta_table = DeltaTable.forPath(spark, full_path)
            history = delta_table.history(10)
            
            # Get table statistics
            table_df = delta_table.toDF()
            row_count = table_df.count()
            
            health_report.append({
                'table': table_path,
                'row_count': row_count,
                'health': 'GOOD'
            })
            
            logger.info(f"Table {table_path}: {row_count} rows")
            
        except Exception as e:
            logger.error(f"Error checking {table_path}: {str(e)}")
            health_report.append({
                'table': table_path,
                'error': str(e),
                'health': 'BAD'
            })
    
    spark.stop()
    
    # Check if any tables are unhealthy
    unhealthy = [t for t in health_report if t.get('health') == 'BAD']
    if unhealthy:
        raise Exception(f"Unhealthy tables detected: {unhealthy}")
    
    return health_report


# Task: Start
start = DummyOperator(
    task_id='start',
    dag=dag,
)

# Task: Check table health before maintenance
check_health_before = PythonOperator(
    task_id='check_table_health_before',
    python_callable=check_table_health,
    provide_context=True,
    dag=dag,
)

# Task Group: Delta Lake Optimization
with TaskGroup('delta_optimization', dag=dag) as optimization_group:
    
    # Create optimization tasks for each table
    for table_path in DELTA_TABLES:
        table_name = table_path.split('/')[-1]
        
        optimize_task = PythonOperator(
            task_id=f'optimize_{table_name}',
            python_callable=optimize_delta_table,
            op_args=[table_path],
            dag=dag,
        )

# Task Group: Delta Lake Vacuum
with TaskGroup('delta_vacuum', dag=dag) as vacuum_group:
    
    # Create vacuum tasks for each table (7 days retention)
    for table_path in DELTA_TABLES:
        table_name = table_path.split('/')[-1]
        
        vacuum_task = PythonOperator(
            task_id=f'vacuum_{table_name}',
            python_callable=vacuum_delta_table,
            op_args=[table_path, 168],  # 7 days = 168 hours
            dag=dag,
        )

# Task: Generate weekly analytics
generate_analytics = PythonOperator(
    task_id='generate_weekly_analytics',
    python_callable=generate_weekly_analytics,
    provide_context=True,
    dag=dag,
)

# Task: Check table health after maintenance
check_health_after = PythonOperator(
    task_id='check_table_health_after',
    python_callable=check_table_health,
    provide_context=True,
    dag=dag,
)

# Task: End
end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Define task dependencies
start >> check_health_before >> optimization_group >> vacuum_group >> generate_analytics >> check_health_after >> end
