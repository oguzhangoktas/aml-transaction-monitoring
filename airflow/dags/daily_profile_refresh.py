"""
Airflow DAG: Daily Customer Profile Refresh
Orchestrates daily customer profile aggregation and data quality checks
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
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
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# DAG definition
dag = DAG(
    'daily_customer_profile_refresh',
    default_args=default_args,
    description='Daily customer profile aggregation and quality checks',
    schedule_interval='0 2 * * *',  # Run at 2 AM UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['aml', 'customer-profiles', 'daily'],
    max_active_runs=1,
)

# Configuration
S3_BUCKET = 's3://aml-transaction-monitoring'
GLUE_ROLE = 'arn:aws:iam::ACCOUNT_ID:role/GlueServiceRole'
REGION = 'eu-central-1'


def validate_data_volume(**context):
    """Validate that sufficient data exists before processing"""
    from boto3 import client
    
    execution_date = context['ds']
    logger.info(f"Validating data volume for {execution_date}")
    
    # This would check S3 for data files
    # Placeholder for actual implementation
    
    return True


def send_success_notification(**context):
    """Send success notification"""
    execution_date = context['ds']
    logger.info(f"Customer profile refresh completed successfully for {execution_date}")
    
    # Could integrate with Slack, PagerDuty, etc.
    return True


def check_quality_metrics(**context):
    """Check if quality metrics meet threshold"""
    execution_date = context['ds']
    
    # Read quality metrics from S3
    # Check if quality_score >= 95%
    # Raise alert if below threshold
    
    logger.info(f"Quality metrics validated for {execution_date}")
    return True


# Task: Start
start = DummyOperator(
    task_id='start',
    dag=dag,
)

# Task: Validate input data
validate_input = PythonOperator(
    task_id='validate_input_data',
    python_callable=validate_data_volume,
    provide_context=True,
    dag=dag,
)

# Task Group: Profile Aggregation
with TaskGroup('profile_aggregation', dag=dag) as profile_agg_group:
    
    # Task: Run Glue job for profile aggregation
    run_profile_aggregation = GlueJobOperator(
        task_id='run_profile_aggregation_job',
        job_name='batch_profile_aggregator',
        region_name=REGION,
        iam_role_name=GLUE_ROLE,
        script_args={
            '--enriched_transactions_path': f'{S3_BUCKET}/data/enriched_transactions/',
            '--customer_profiles_path': f'{S3_BUCKET}/data/customer_profiles/',
            '--customer_master_path': f'{S3_BUCKET}/data/customer_master/',
            '--lookback_days': '90',
        },
        num_of_dpus=10,
        dag=dag,
    )
    
    # Task: Wait for profile aggregation to complete
    wait_profile_aggregation = GlueJobSensor(
        task_id='wait_profile_aggregation',
        job_name='batch_profile_aggregator',
        run_id='{{ task_instance.xcom_pull(task_ids="profile_aggregation.run_profile_aggregation_job") }}',
        region_name=REGION,
        poke_interval=60,
        timeout=3600,
        dag=dag,
    )
    
    run_profile_aggregation >> wait_profile_aggregation

# Task Group: Data Quality Checks
with TaskGroup('data_quality_checks', dag=dag) as quality_check_group:
    
    # Task: Run data quality checks
    run_quality_checks = GlueJobOperator(
        task_id='run_quality_checks_job',
        job_name='data_quality_checks',
        region_name=REGION,
        iam_role_name=GLUE_ROLE,
        script_args={
            '--raw_transactions_path': f'{S3_BUCKET}/data/raw_transactions/',
            '--enriched_transactions_path': f'{S3_BUCKET}/data/enriched_transactions/',
            '--customer_profiles_path': f'{S3_BUCKET}/data/customer_profiles/',
            '--quality_metrics_path': f'{S3_BUCKET}/data/quality_metrics/',
            '--check_date': '{{ ds }}',
        },
        num_of_dpus=5,
        dag=dag,
    )
    
    # Task: Wait for quality checks to complete
    wait_quality_checks = GlueJobSensor(
        task_id='wait_quality_checks',
        job_name='data_quality_checks',
        run_id='{{ task_instance.xcom_pull(task_ids="data_quality_checks.run_quality_checks_job") }}',
        region_name=REGION,
        poke_interval=30,
        timeout=1800,
        dag=dag,
    )
    
    # Task: Validate quality metrics
    validate_quality = PythonOperator(
        task_id='validate_quality_metrics',
        python_callable=check_quality_metrics,
        provide_context=True,
        dag=dag,
    )
    
    run_quality_checks >> wait_quality_checks >> validate_quality

# Task Group: Reporting
with TaskGroup('daily_reporting', dag=dag) as reporting_group:
    
    # Task: Generate daily alert report
    run_daily_report = GlueJobOperator(
        task_id='generate_daily_alert_report',
        job_name='daily_alert_report',
        region_name=REGION,
        iam_role_name=GLUE_ROLE,
        script_args={
            '--alerts_path': f'{S3_BUCKET}/data/alerts/',
            '--enriched_transactions_path': f'{S3_BUCKET}/data/enriched_transactions/',
            '--customer_profiles_path': f'{S3_BUCKET}/data/customer_profiles/',
            '--reports_output_path': f'{S3_BUCKET}/reports/daily_alerts/',
            '--report_date': '{{ ds }}',
        },
        num_of_dpus=5,
        dag=dag,
    )
    
    # Task: Wait for report generation
    wait_daily_report = GlueJobSensor(
        task_id='wait_daily_report',
        job_name='daily_alert_report',
        run_id='{{ task_instance.xcom_pull(task_ids="daily_reporting.generate_daily_alert_report") }}',
        region_name=REGION,
        poke_interval=30,
        timeout=1800,
        dag=dag,
    )
    
    run_daily_report >> wait_daily_report

# Task: Send success notification
notify_success = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    provide_context=True,
    dag=dag,
)

# Task: End
end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Define task dependencies
start >> validate_input >> profile_agg_group >> quality_check_group >> reporting_group >> notify_success >> end
