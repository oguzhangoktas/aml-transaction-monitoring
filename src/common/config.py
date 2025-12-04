"""
Configuration Management for AML Transaction Monitoring Pipeline

Centralized configuration for all environments (local, dev, prod)
"""

import os
from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class KafkaConfig:
    """Kafka connection and topic configuration"""
    bootstrap_servers: str
    topic_transactions: str
    topic_alerts: str
    consumer_group_id: str
    auto_offset_reset: str = "latest"
    enable_auto_commit: bool = False
    max_poll_records: int = 500
    session_timeout_ms: int = 30000
    
    @classmethod
    def from_env(cls) -> "KafkaConfig":
        """Create config from environment variables"""
        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic_transactions=os.getenv("KAFKA_TOPIC_TRANSACTIONS", "transactions-raw"),
            topic_alerts=os.getenv("KAFKA_TOPIC_ALERTS", "alerts-high-priority"),
            consumer_group_id=os.getenv("KAFKA_CONSUMER_GROUP", "aml-processor-group"),
        )


@dataclass
class SparkConfig:
    """Spark job configuration"""
    app_name: str
    master: str
    executor_memory: str
    driver_memory: str
    executor_cores: int
    shuffle_partitions: int
    
    # Delta Lake settings
    enable_delta: bool = True
    delta_log_cache_size: int = 1000
    
    # Streaming settings
    streaming_trigger_interval: str = "10 seconds"
    streaming_checkpoint_location: str = None
    
    @classmethod
    def from_env(cls) -> "SparkConfig":
        """Create config from environment variables"""
        return cls(
            app_name=os.getenv("SPARK_APP_NAME", "aml-transaction-monitoring"),
            master=os.getenv("SPARK_MASTER", "local[*]"),
            executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", "4g"),
            driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "4g"),
            executor_cores=int(os.getenv("SPARK_EXECUTOR_CORES", "2")),
            shuffle_partitions=int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "200")),
            streaming_checkpoint_location=os.getenv("CHECKPOINT_PATH", "./data/checkpoints"),
        )
    
    def to_spark_conf(self) -> Dict[str, str]:
        """Convert to Spark configuration dictionary"""
        conf = {
            "spark.app.name": self.app_name,
            "spark.executor.memory": self.executor_memory,
            "spark.driver.memory": self.driver_memory,
            "spark.executor.cores": str(self.executor_cores),
            "spark.sql.shuffle.partitions": str(self.shuffle_partitions),
        }
        
        if self.enable_delta:
            conf.update({
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.databricks.delta.retentionDurationCheck.enabled": "false",
                "spark.databricks.delta.properties.defaults.enableChangeDataFeed": "true",
            })
        
        return conf


@dataclass
class DeltaLakeConfig:
    """Delta Lake storage paths"""
    base_path: str
    bronze_transactions_path: str
    silver_transactions_path: str
    gold_alerts_path: str
    gold_customer_summary_path: str
    
    @classmethod
    def from_env(cls) -> "DeltaLakeConfig":
        """Create config from environment variables"""
        base_path = os.getenv("DELTA_LAKE_PATH", "./data/delta")
        
        return cls(
            base_path=base_path,
            bronze_transactions_path=f"{base_path}/bronze/transactions",
            silver_transactions_path=f"{base_path}/silver/transactions",
            gold_alerts_path=f"{base_path}/gold/alerts",
            gold_customer_summary_path=f"{base_path}/gold/customer_daily_summary",
        )


@dataclass
class AWSConfig:
    """AWS service configuration"""
    region: str
    s3_bucket_data_lake: str
    s3_bucket_scripts: str
    glue_database: str
    redshift_cluster: str
    redshift_database: str
    redshift_user: str
    redshift_password: str
    
    @classmethod
    def from_env(cls) -> "AWSConfig":
        """Create config from environment variables"""
        return cls(
            region=os.getenv("AWS_REGION", "eu-central-1"),
            s3_bucket_data_lake=os.getenv("S3_BUCKET_DATA_LAKE", ""),
            s3_bucket_scripts=os.getenv("S3_BUCKET_SCRIPTS", ""),
            glue_database=os.getenv("GLUE_DATABASE", "aml_database"),
            redshift_cluster=os.getenv("REDSHIFT_CLUSTER", ""),
            redshift_database=os.getenv("REDSHIFT_DATABASE", "aml_dw"),
            redshift_user=os.getenv("REDSHIFT_USER", "admin"),
            redshift_password=os.getenv("REDSHIFT_PASSWORD", ""),
        )


@dataclass
class BusinessRulesConfig:
    """AML business rules thresholds"""
    # High value transaction rule
    high_value_threshold_eur: float = 10000.0
    
    # Rapid movement rule (24-hour window)
    rapid_movement_threshold_eur: float = 15000.0
    rapid_movement_window_hours: int = 24
    
    # Structuring detection
    structuring_threshold_eur: float = 9000.0  # Just below reporting threshold
    structuring_count_threshold: int = 3
    structuring_window_hours: int = 24
    
    # Velocity anomaly
    velocity_multiplier: float = 3.0  # 3x normal daily count
    
    # Risk scoring
    base_risk_score: float = 50.0
    high_value_score_increment: float = 20.0
    high_risk_country_score_increment: float = 30.0
    pep_customer_score_increment: float = 25.0
    
    @classmethod
    def from_env(cls) -> "BusinessRulesConfig":
        """Create config from environment variables"""
        return cls(
            high_value_threshold_eur=float(os.getenv("HIGH_VALUE_THRESHOLD", "10000")),
            rapid_movement_threshold_eur=float(os.getenv("RAPID_MOVEMENT_THRESHOLD", "15000")),
            structuring_threshold_eur=float(os.getenv("STRUCTURING_THRESHOLD", "9000")),
        )


@dataclass
class DataQualityConfig:
    """Data quality thresholds"""
    completeness_threshold: float = 0.999  # 99.9%
    accuracy_threshold: float = 0.999
    timeliness_threshold_seconds: int = 3
    max_processing_lag_minutes: int = 5
    
    @classmethod
    def from_env(cls) -> "DataQualityConfig":
        """Create config from environment variables"""
        return cls(
            completeness_threshold=float(os.getenv("DQ_COMPLETENESS_THRESHOLD", "0.999")),
            accuracy_threshold=float(os.getenv("DQ_ACCURACY_THRESHOLD", "0.999")),
        )


@dataclass
class Config:
    """Master configuration"""
    environment: str
    kafka: KafkaConfig
    spark: SparkConfig
    delta_lake: DeltaLakeConfig
    aws: AWSConfig
    business_rules: BusinessRulesConfig
    data_quality: DataQualityConfig
    
    # Logging
    log_level: str = "INFO"
    
    @classmethod
    def load(cls, environment: str = None) -> "Config":
        """
        Load configuration for specified environment
        
        Args:
            environment: Environment name (local, dev, prod)
                        If None, reads from ENVIRONMENT env var
        """
        if environment is None:
            environment = os.getenv("ENVIRONMENT", "local")
        
        return cls(
            environment=environment,
            kafka=KafkaConfig.from_env(),
            spark=SparkConfig.from_env(),
            delta_lake=DeltaLakeConfig.from_env(),
            aws=AWSConfig.from_env(),
            business_rules=BusinessRulesConfig.from_env(),
            data_quality=DataQualityConfig.from_env(),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )
    
    def is_production(self) -> bool:
        """Check if running in production"""
        return self.environment.lower() == "production"
    
    def is_local(self) -> bool:
        """Check if running locally"""
        return self.environment.lower() == "local"


# ============================================================================
# Global config instance
# ============================================================================

# Load config on module import
config = Config.load()


# ============================================================================
# Helper functions
# ============================================================================

def get_config() -> Config:
    """Get global config instance"""
    return config


def reload_config(environment: str = None):
    """Reload configuration"""
    global config
    config = Config.load(environment)


def print_config():
    """Print current configuration (masks sensitive data)"""
    print("\n" + "="*60)
    print("AML Pipeline Configuration")
    print("="*60)
    
    print(f"\nEnvironment: {config.environment}")
    
    print(f"\nKafka:")
    print(f"  Bootstrap Servers: {config.kafka.bootstrap_servers}")
    print(f"  Transactions Topic: {config.kafka.topic_transactions}")
    print(f"  Alerts Topic: {config.kafka.topic_alerts}")
    
    print(f"\nSpark:")
    print(f"  App Name: {config.spark.app_name}")
    print(f"  Master: {config.spark.master}")
    print(f"  Executor Memory: {config.spark.executor_memory}")
    print(f"  Shuffle Partitions: {config.spark.shuffle_partitions}")
    
    print(f"\nDelta Lake:")
    print(f"  Base Path: {config.delta_lake.base_path}")
    print(f"  Bronze Path: {config.delta_lake.bronze_transactions_path}")
    print(f"  Silver Path: {config.delta_lake.silver_transactions_path}")
    print(f"  Gold Alerts Path: {config.delta_lake.gold_alerts_path}")
    
    print(f"\nAWS:")
    print(f"  Region: {config.aws.region}")
    print(f"  S3 Data Lake: {config.aws.s3_bucket_data_lake or 'Not configured (local mode)'}")
    print(f"  Glue Database: {config.aws.glue_database}")
    
    print(f"\nBusiness Rules:")
    print(f"  High Value Threshold: €{config.business_rules.high_value_threshold_eur:,.2f}")
    print(f"  Rapid Movement Threshold: €{config.business_rules.rapid_movement_threshold_eur:,.2f}")
    print(f"  Structuring Threshold: €{config.business_rules.structuring_threshold_eur:,.2f}")
    
    print(f"\nData Quality:")
    print(f"  Completeness Threshold: {config.data_quality.completeness_threshold*100:.2f}%")
    print(f"  Accuracy Threshold: {config.data_quality.accuracy_threshold*100:.2f}%")
    print(f"  Timeliness SLA: {config.data_quality.timeliness_threshold_seconds}s")
    
    print("="*60 + "\n")


if __name__ == "__main__":
    # Print configuration when run directly
    print_config()
