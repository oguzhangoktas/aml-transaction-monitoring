"""
AWS Glue Job: Data Quality Checks
Validates data quality across the pipeline and generates quality metrics
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import logging

# Initialize Glue context
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'raw_transactions_path',
    'enriched_transactions_path',
    'customer_profiles_path',
    'quality_metrics_path',
    'check_date'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class DataQualityChecker:
    """Performs comprehensive data quality checks"""
    
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.check_date = datetime.strptime(config['check_date'], '%Y-%m-%d').date()
        self.quality_results = []
        
    def check_completeness(self, df: DataFrame, table_name: str, required_cols: list) -> dict:
        """Check completeness - no null values in required columns"""
        total_rows = df.count()
        results = {
            'table_name': table_name,
            'check_type': 'COMPLETENESS',
            'check_date': self.check_date,
            'total_rows': total_rows,
            'checks': []
        }
        
        for col in required_cols:
            null_count = df.filter(F.col(col).isNull()).count()
            null_percentage = (null_count / total_rows * 100) if total_rows > 0 else 0
            
            check_result = {
                'column': col,
                'null_count': null_count,
                'null_percentage': round(null_percentage, 2),
                'passed': null_count == 0
            }
            results['checks'].append(check_result)
        
        logger.info(f"Completeness check completed for {table_name}")
        return results
    
    def check_validity(self, df: DataFrame, table_name: str, validation_rules: dict) -> dict:
        """Check validity - values meet business rules"""
        total_rows = df.count()
        results = {
            'table_name': table_name,
            'check_type': 'VALIDITY',
            'check_date': self.check_date,
            'total_rows': total_rows,
            'checks': []
        }
        
        for rule_name, condition in validation_rules.items():
            invalid_count = df.filter(~condition).count()
            invalid_percentage = (invalid_count / total_rows * 100) if total_rows > 0 else 0
            
            check_result = {
                'rule': rule_name,
                'invalid_count': invalid_count,
                'invalid_percentage': round(invalid_percentage, 2),
                'passed': invalid_count == 0
            }
            results['checks'].append(check_result)
        
        logger.info(f"Validity check completed for {table_name}")
        return results
    
    def check_consistency(self, df: DataFrame, table_name: str) -> dict:
        """Check consistency across related fields"""
        total_rows = df.count()
        results = {
            'table_name': table_name,
            'check_type': 'CONSISTENCY',
            'check_date': self.check_date,
            'total_rows': total_rows,
            'checks': []
        }
        
        # Example: Check if transaction_timestamp <= processing_timestamp
        if 'transaction_timestamp' in df.columns and 'processing_timestamp' in df.columns:
            inconsistent = df.filter(
                F.col("transaction_timestamp") > F.col("processing_timestamp")
            ).count()
            
            results['checks'].append({
                'rule': 'transaction_timestamp_before_processing',
                'inconsistent_count': inconsistent,
                'passed': inconsistent == 0
            })
        
        # Check if amount > 0
        if 'amount' in df.columns:
            negative_amounts = df.filter(F.col("amount") <= 0).count()
            results['checks'].append({
                'rule': 'positive_amounts',
                'inconsistent_count': negative_amounts,
                'passed': negative_amounts == 0
            })
        
        logger.info(f"Consistency check completed for {table_name}")
        return results
    
    def check_uniqueness(self, df: DataFrame, table_name: str, unique_keys: list) -> dict:
        """Check uniqueness of key columns"""
        total_rows = df.count()
        results = {
            'table_name': table_name,
            'check_type': 'UNIQUENESS',
            'check_date': self.check_date,
            'total_rows': total_rows,
            'checks': []
        }
        
        for key_col in unique_keys:
            distinct_count = df.select(key_col).distinct().count()
            duplicate_count = total_rows - distinct_count
            
            results['checks'].append({
                'column': key_col,
                'distinct_count': distinct_count,
                'duplicate_count': duplicate_count,
                'passed': duplicate_count == 0
            })
        
        logger.info(f"Uniqueness check completed for {table_name}")
        return results
    
    def check_timeliness(self, df: DataFrame, table_name: str, timestamp_col: str) -> dict:
        """Check data freshness"""
        results = {
            'table_name': table_name,
            'check_type': 'TIMELINESS',
            'check_date': self.check_date,
            'checks': []
        }
        
        # Get latest timestamp
        latest_timestamp = df.agg(F.max(timestamp_col)).collect()[0][0]
        current_time = datetime.now()
        
        if latest_timestamp:
            lag_hours = (current_time - latest_timestamp).total_seconds() / 3600
            
            results['checks'].append({
                'metric': 'data_freshness',
                'latest_timestamp': latest_timestamp.isoformat(),
                'lag_hours': round(lag_hours, 2),
                'passed': lag_hours < 24  # Data should be < 24 hours old
            })
        
        logger.info(f"Timeliness check completed for {table_name}")
        return results
    
    def check_volume(self, df: DataFrame, table_name: str, expected_min: int, expected_max: int) -> dict:
        """Check data volume is within expected range"""
        actual_count = df.count()
        
        results = {
            'table_name': table_name,
            'check_type': 'VOLUME',
            'check_date': self.check_date,
            'checks': [{
                'metric': 'row_count',
                'actual_count': actual_count,
                'expected_min': expected_min,
                'expected_max': expected_max,
                'passed': expected_min <= actual_count <= expected_max
            }]
        }
        
        logger.info(f"Volume check completed for {table_name}: {actual_count} rows")
        return results
    
    def check_raw_transactions(self) -> None:
        """Run quality checks on raw transactions"""
        df = self.spark.read.format("delta") \
            .load(self.config['raw_transactions_path']) \
            .filter(F.col("processing_date") == self.check_date)
        
        # Completeness
        required_cols = ['transaction_id', 'customer_id', 'amount', 'currency', 'timestamp']
        self.quality_results.append(
            self.check_completeness(df, 'raw_transactions', required_cols)
        )
        
        # Validity
        validation_rules = {
            'valid_currency': F.col('currency').isin(['EUR', 'USD', 'GBP', 'CHF', 'AED', 'RUB', 'CNY', 'TRY']),
            'valid_transaction_type': F.col('transaction_type').isin([
                'ATM_WITHDRAWAL', 'POS_PURCHASE', 'ONLINE_TRANSFER', 
                'WIRE_TRANSFER', 'CHECK_DEPOSIT', 'CASH_DEPOSIT'
            ]),
            'positive_amount': F.col('amount') > 0
        }
        self.quality_results.append(
            self.check_validity(df, 'raw_transactions', validation_rules)
        )
        
        # Uniqueness
        self.quality_results.append(
            self.check_uniqueness(df, 'raw_transactions', ['transaction_id'])
        )
        
        # Consistency
        self.quality_results.append(
            self.check_consistency(df, 'raw_transactions')
        )
        
        # Volume (expect 1M-10M transactions per day)
        self.quality_results.append(
            self.check_volume(df, 'raw_transactions', 1000000, 10000000)
        )
    
    def check_enriched_transactions(self) -> None:
        """Run quality checks on enriched transactions"""
        df = self.spark.read.format("delta") \
            .load(self.config['enriched_transactions_path']) \
            .filter(F.col("processing_date") == self.check_date)
        
        # Completeness
        required_cols = [
            'transaction_id', 'customer_id', 'amount', 
            'transaction_risk_score', 'is_suspicious'
        ]
        self.quality_results.append(
            self.check_completeness(df, 'enriched_transactions', required_cols)
        )
        
        # Validity
        validation_rules = {
            'risk_score_range': (F.col('transaction_risk_score') >= 0) & (F.col('transaction_risk_score') <= 1),
            'valid_priority': F.col('alert_priority').isin(['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']),
            'positive_rules': F.col('total_rules_triggered') >= 0
        }
        self.quality_results.append(
            self.check_validity(df, 'enriched_transactions', validation_rules)
        )
        
        # Consistency
        self.quality_results.append(
            self.check_consistency(df, 'enriched_transactions')
        )
    
    def check_customer_profiles(self) -> None:
        """Run quality checks on customer profiles"""
        df = self.spark.read.format("delta") \
            .load(self.config['customer_profiles_path']) \
            .filter(F.col("is_current") == True)
        
        # Completeness
        required_cols = ['customer_id', 'risk_score', 'risk_category']
        self.quality_results.append(
            self.check_completeness(df, 'customer_profiles', required_cols)
        )
        
        # Validity
        validation_rules = {
            'risk_score_range': (F.col('risk_score') >= 0) & (F.col('risk_score') <= 1),
            'valid_risk_category': F.col('risk_category').isin(['LOW', 'MEDIUM', 'HIGH']),
            'valid_kyc': F.col('kyc_status').isin(['APPROVED', 'PENDING', 'REJECTED'])
        }
        self.quality_results.append(
            self.check_validity(df, 'customer_profiles', validation_rules)
        )
        
        # Uniqueness
        self.quality_results.append(
            self.check_uniqueness(df, 'customer_profiles', ['customer_id'])
        )
        
        # Timeliness
        self.quality_results.append(
            self.check_timeliness(df, 'customer_profiles', 'profile_timestamp')
        )
    
    def calculate_summary_metrics(self) -> DataFrame:
        """Calculate overall quality metrics"""
        total_checks = 0
        passed_checks = 0
        
        for result in self.quality_results:
            for check in result.get('checks', []):
                total_checks += 1
                if check.get('passed', False):
                    passed_checks += 1
        
        quality_score = (passed_checks / total_checks * 100) if total_checks > 0 else 0
        
        summary = self.spark.createDataFrame([
            (
                self.check_date,
                datetime.now(),
                total_checks,
                passed_checks,
                total_checks - passed_checks,
                round(quality_score, 2),
                'PASS' if quality_score >= 95 else 'FAIL'
            )
        ], [
            'check_date', 'check_timestamp', 'total_checks', 
            'passed_checks', 'failed_checks', 'quality_score', 'overall_status'
        ])
        
        logger.info(f"Quality Score: {quality_score}%")
        return summary
    
    def write_results(self, summary_df: DataFrame) -> None:
        """Write quality check results to S3"""
        check_date_str = self.check_date.strftime('%Y-%m-%d')
        output_path = f"{self.config['quality_metrics_path']}/check_date={check_date_str}"
        
        # Convert results to DataFrame
        results_data = []
        for result in self.quality_results:
            for check in result.get('checks', []):
                results_data.append({
                    'check_date': result['check_date'],
                    'table_name': result['table_name'],
                    'check_type': result['check_type'],
                    'check_detail': str(check),
                    'passed': check.get('passed', False),
                    'timestamp': datetime.now()
                })
        
        if results_data:
            results_df = self.spark.createDataFrame(results_data)
            results_df.write \
                .format("parquet") \
                .mode("overwrite") \
                .save(f"{output_path}/details")
        
        # Write summary
        summary_df.write \
            .format("parquet") \
            .mode("overwrite") \
            .save(f"{output_path}/summary")
        
        logger.info(f"Quality check results written to {output_path}")


def main():
    """Main execution function"""
    config = {
        'raw_transactions_path': args['raw_transactions_path'],
        'enriched_transactions_path': args['enriched_transactions_path'],
        'customer_profiles_path': args['customer_profiles_path'],
        'quality_metrics_path': args['quality_metrics_path'],
        'check_date': args['check_date']
    }
    
    checker = DataQualityChecker(spark, config)
    
    # Run all quality checks
    checker.check_raw_transactions()
    checker.check_enriched_transactions()
    checker.check_customer_profiles()
    
    # Calculate and write results
    summary = checker.calculate_summary_metrics()
    checker.write_results(summary)
    
    # Print summary
    summary.show(truncate=False)
    
    logger.info("Data quality checks completed successfully")
    job.commit()


if __name__ == "__main__":
    main()
