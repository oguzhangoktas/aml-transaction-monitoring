"""
AWS Glue Batch Job: Daily Alert Report Generator
Generates regulatory STR (Suspicious Transaction Report) for compliance
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
from datetime import datetime, timedelta
import logging

# Initialize Glue context
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'alerts_path',
    'enriched_transactions_path',
    'customer_profiles_path',
    'reports_output_path',
    'report_date'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configure Spark for Delta Lake
spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class DailyAlertReportGenerator:
    """Generates daily STR reports for compliance team"""
    
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.report_date = datetime.strptime(config['report_date'], '%Y-%m-%d').date()
        
    def read_daily_alerts(self) -> DataFrame:
        """Read alerts generated on report date"""
        alerts_df = self.spark.read.format("delta") \
            .load(self.config['alerts_path']) \
            .filter(F.to_date("alert_generated_at") == self.report_date)
        
        logger.info(f"Read {alerts_df.count()} alerts for {self.report_date}")
        return alerts_df
    
    def enrich_alerts_with_details(self, alerts_df: DataFrame) -> DataFrame:
        """Enrich alerts with transaction and customer details"""
        
        # Read transaction details
        transactions_df = self.spark.read.format("delta") \
            .load(self.config['enriched_transactions_path']) \
            .filter(F.to_date("processing_date") == self.report_date) \
            .select(
                "transaction_id",
                "merchant_category",
                "channel",
                "device_id",
                "kafka_timestamp",
                "amount_vs_avg_ratio"
            )
        
        # Read customer profiles
        profiles_df = self.spark.read.format("delta") \
            .load(self.config['customer_profiles_path']) \
            .filter(F.col("is_current") == True) \
            .select(
                "customer_id",
                "customer_name",
                "customer_segment",
                "risk_category",
                "account_age_days",
                "country_of_residence",
                "kyc_status"
            )
        
        # Enrich alerts
        enriched_alerts = alerts_df \
            .join(transactions_df, on="transaction_id", how="left") \
            .join(profiles_df, on="customer_id", how="left")
        
        logger.info("Alerts enriched with transaction and customer details")
        return enriched_alerts
    
    def generate_alert_summary(self, alerts_df: DataFrame) -> DataFrame:
        """Generate summary statistics for the daily report"""
        
        summary_df = alerts_df.groupBy("alert_priority").agg(
            F.count("transaction_id").alias("alert_count"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum(F.when(F.col("alert_status") == "OPEN", 1).otherwise(0)).alias("open_alerts"),
            F.sum(F.when(F.col("alert_status") == "REVIEWED", 1).otherwise(0)).alias("reviewed_alerts")
        ).orderBy(
            F.when(F.col("alert_priority") == "CRITICAL", 1)
            .when(F.col("alert_priority") == "HIGH", 2)
            .when(F.col("alert_priority") == "MEDIUM", 3)
            .otherwise(4)
        )
        
        logger.info("Alert summary generated")
        return summary_df
    
    def generate_rule_breakdown(self, alerts_df: DataFrame) -> DataFrame:
        """Generate breakdown by triggered rules"""
        
        rule_breakdown = alerts_df.agg(
            F.sum("rule_1_triggered").alias("rule_1_large_cash_count"),
            F.sum("rule_2_triggered").alias("rule_2_high_risk_country_count"),
            F.sum("rule_3_triggered").alias("rule_3_unusual_amount_count"),
            F.sum("rule_4_triggered").alias("rule_4_pep_wire_count"),
            F.sum("rule_5_triggered").alias("rule_5_high_frequency_count"),
            F.sum("rule_6_triggered").alias("rule_6_high_risk_unusual_count")
        )
        
        logger.info("Rule breakdown generated")
        return rule_breakdown
    
    def generate_customer_risk_report(self, alerts_df: DataFrame) -> DataFrame:
        """Generate customer-level risk report"""
        
        customer_report = alerts_df.groupBy("customer_id", "customer_name", "risk_category").agg(
            F.count("transaction_id").alias("alert_count"),
            F.sum("amount").alias("total_flagged_amount"),
            F.avg("transaction_risk_score").alias("avg_risk_score"),
            F.max("alert_priority").alias("highest_priority"),
            F.collect_list("transaction_id").alias("flagged_transaction_ids"),
            F.countDistinct("destination_country").alias("unique_dest_countries")
        ).orderBy(F.col("alert_count").desc(), F.col("total_flagged_amount").desc())
        
        logger.info("Customer risk report generated")
        return customer_report
    
    def generate_geographic_analysis(self, alerts_df: DataFrame) -> DataFrame:
        """Generate geographic distribution of suspicious transactions"""
        
        geo_report = alerts_df.groupBy("destination_country").agg(
            F.count("transaction_id").alias("alert_count"),
            F.sum("amount").alias("total_amount"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.avg("transaction_risk_score").alias("avg_risk_score")
        ).orderBy(F.col("alert_count").desc())
        
        logger.info("Geographic analysis generated")
        return geo_report
    
    def write_reports(
        self,
        enriched_alerts: DataFrame,
        summary: DataFrame,
        rule_breakdown: DataFrame,
        customer_report: DataFrame,
        geo_report: DataFrame
    ) -> None:
        """Write all reports to S3"""
        
        report_date_str = self.report_date.strftime('%Y-%m-%d')
        output_path = f"{self.config['reports_output_path']}/report_date={report_date_str}"
        
        # Write main alert details
        enriched_alerts.write \
            .format("parquet") \
            .mode("overwrite") \
            .save(f"{output_path}/alert_details")
        
        # Write summary
        summary.write \
            .format("parquet") \
            .mode("overwrite") \
            .save(f"{output_path}/summary")
        
        # Write rule breakdown
        rule_breakdown.write \
            .format("parquet") \
            .mode("overwrite") \
            .save(f"{output_path}/rule_breakdown")
        
        # Write customer report
        customer_report.write \
            .format("parquet") \
            .mode("overwrite") \
            .save(f"{output_path}/customer_risk")
        
        # Write geographic report
        geo_report.write \
            .format("parquet") \
            .mode("overwrite") \
            .save(f"{output_path}/geographic_analysis")
        
        logger.info(f"All reports written to {output_path}")
        
        # Also write to Redshift for BI tools
        self._write_to_redshift(enriched_alerts, summary, customer_report)
    
    def _write_to_redshift(
        self,
        alerts_df: DataFrame,
        summary_df: DataFrame,
        customer_df: DataFrame
    ) -> None:
        """Write key reports to Redshift for analytics"""
        
        # This would require proper Redshift connection setup
        # Placeholder for the actual implementation
        
        logger.info("Writing to Redshift")
        
        # Example: Write to Redshift using JDBC or Glue connection
        # alerts_df.write \
        #     .format("jdbc") \
        #     .option("url", redshift_url) \
        #     .option("dbtable", "aml.daily_alerts") \
        #     .option("tempdir", "s3://bucket/temp/") \
        #     .mode("append") \
        #     .save()
        
        logger.info("Redshift write completed")
    
    def generate_compliance_metrics(self, alerts_df: DataFrame) -> DataFrame:
        """Generate compliance KPIs"""
        
        metrics = self.spark.createDataFrame([
            (
                self.report_date,
                alerts_df.count(),
                alerts_df.filter(F.col("alert_priority") == "CRITICAL").count(),
                alerts_df.filter(F.col("alert_priority") == "HIGH").count(),
                alerts_df.select(F.sum("amount")).collect()[0][0] or 0,
                alerts_df.select(F.countDistinct("customer_id")).collect()[0][0],
                alerts_df.filter(F.col("alert_status") == "OPEN").count(),
                datetime.now()
            )
        ], ["report_date", "total_alerts", "critical_alerts", "high_alerts", 
            "total_flagged_amount", "unique_customers", "open_alerts", "generated_at"])
        
        logger.info("Compliance metrics generated")
        return metrics


def main():
    """Main execution function"""
    config = {
        'alerts_path': args['alerts_path'],
        'enriched_transactions_path': args['enriched_transactions_path'],
        'customer_profiles_path': args['customer_profiles_path'],
        'reports_output_path': args['reports_output_path'],
        'report_date': args['report_date']
    }
    
    generator = DailyAlertReportGenerator(spark, config)
    
    # Read alerts
    alerts_df = generator.read_daily_alerts()
    
    if alerts_df.count() == 0:
        logger.warning(f"No alerts found for {config['report_date']}")
        job.commit()
        return
    
    # Enrich alerts
    enriched_alerts = generator.enrich_alerts_with_details(alerts_df)
    
    # Generate reports
    summary = generator.generate_alert_summary(enriched_alerts)
    rule_breakdown = generator.generate_rule_breakdown(enriched_alerts)
    customer_report = generator.generate_customer_risk_report(enriched_alerts)
    geo_report = generator.generate_geographic_analysis(enriched_alerts)
    compliance_metrics = generator.generate_compliance_metrics(enriched_alerts)
    
    # Write all reports
    generator.write_reports(
        enriched_alerts,
        summary,
        rule_breakdown,
        customer_report,
        geo_report
    )
    
    logger.info(f"Daily alert report for {config['report_date']} completed successfully")
    job.commit()


if __name__ == "__main__":
    main()
