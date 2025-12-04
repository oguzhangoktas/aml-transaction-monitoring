"""
Delta Lake Utility Functions
Handles Delta Lake operations, optimizations, and maintenance
"""
from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)


class DeltaLakeManager:
    """Manages Delta Lake operations and optimizations"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    def upsert_data(
        self,
        source_df: DataFrame,
        target_path: str,
        merge_keys: List[str],
        partition_cols: Optional[List[str]] = None
    ) -> None:
        """
        Upsert data into Delta Lake table
        
        Args:
            source_df: Source DataFrame
            target_path: S3 path to Delta table
            merge_keys: Keys for merge condition
            partition_cols: Partition columns
        """
        try:
            # Check if table exists
            if DeltaTable.isDeltaTable(self.spark, target_path):
                delta_table = DeltaTable.forPath(self.spark, target_path)
                
                # Build merge condition
                merge_condition = " AND ".join([
                    f"target.{key} = source.{key}" for key in merge_keys
                ])
                
                # Perform merge
                delta_table.alias("target").merge(
                    source_df.alias("source"),
                    merge_condition
                ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                
                logger.info(f"Upserted data to {target_path}")
            else:
                # First time write
                writer = source_df.write.format("delta").mode("overwrite")
                if partition_cols:
                    writer = writer.partitionBy(*partition_cols)
                writer.save(target_path)
                logger.info(f"Created new Delta table at {target_path}")
                
        except Exception as e:
            logger.error(f"Error upserting data: {str(e)}")
            raise
            
    def append_data(
        self,
        df: DataFrame,
        target_path: str,
        partition_cols: Optional[List[str]] = None
    ) -> None:
        """
        Append data to Delta Lake table
        
        Args:
            df: DataFrame to append
            target_path: S3 path to Delta table
            partition_cols: Partition columns
        """
        try:
            writer = df.write.format("delta").mode("append")
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            writer.save(target_path)
            logger.info(f"Appended data to {target_path}")
        except Exception as e:
            logger.error(f"Error appending data: {str(e)}")
            raise
            
    def optimize_table(
        self,
        table_path: str,
        z_order_cols: Optional[List[str]] = None
    ) -> None:
        """
        Optimize Delta table with compaction and Z-ordering
        
        Args:
            table_path: S3 path to Delta table
            z_order_cols: Columns for Z-ordering
        """
        try:
            delta_table = DeltaTable.forPath(self.spark, table_path)
            
            # Compact small files
            optimize_cmd = delta_table.optimize()
            
            # Apply Z-ordering if specified
            if z_order_cols:
                optimize_cmd = optimize_cmd.executeZOrderBy(z_order_cols)
            else:
                optimize_cmd.executeCompaction()
                
            logger.info(f"Optimized table at {table_path}")
        except Exception as e:
            logger.error(f"Error optimizing table: {str(e)}")
            raise
            
    def vacuum_table(
        self,
        table_path: str,
        retention_hours: int = 168  # 7 days default
    ) -> None:
        """
        Clean up old versions of Delta table
        
        Args:
            table_path: S3 path to Delta table
            retention_hours: Hours to retain old versions
        """
        try:
            delta_table = DeltaTable.forPath(self.spark, table_path)
            delta_table.vacuum(retention_hours)
            logger.info(f"Vacuumed table at {table_path} with {retention_hours}h retention")
        except Exception as e:
            logger.error(f"Error vacuuming table: {str(e)}")
            raise
            
    def read_delta_table(
        self,
        table_path: str,
        version: Optional[int] = None,
        timestamp: Optional[str] = None
    ) -> DataFrame:
        """
        Read Delta table with optional time travel
        
        Args:
            table_path: S3 path to Delta table
            version: Specific version to read
            timestamp: Timestamp to read (format: 'YYYY-MM-DD HH:MM:SS')
            
        Returns:
            DataFrame
        """
        try:
            reader = self.spark.read.format("delta")
            
            if version is not None:
                reader = reader.option("versionAsOf", version)
            elif timestamp is not None:
                reader = reader.option("timestampAsOf", timestamp)
                
            df = reader.load(table_path)
            logger.info(f"Read Delta table from {table_path}")
            return df
        except Exception as e:
            logger.error(f"Error reading Delta table: {str(e)}")
            raise
            
    def get_table_history(self, table_path: str) -> DataFrame:
        """
        Get Delta table history
        
        Args:
            table_path: S3 path to Delta table
            
        Returns:
            DataFrame with table history
        """
        try:
            delta_table = DeltaTable.forPath(self.spark, table_path)
            history_df = delta_table.history()
            return history_df
        except Exception as e:
            logger.error(f"Error getting table history: {str(e)}")
            raise
            
    def create_scd_type2_table(
        self,
        source_df: DataFrame,
        target_path: str,
        business_keys: List[str],
        partition_cols: Optional[List[str]] = None
    ) -> None:
        """
        Create or update SCD Type 2 table
        
        Args:
            source_df: Source DataFrame with new/updated records
            target_path: S3 path to Delta table
            business_keys: Business key columns
            partition_cols: Partition columns
        """
        from pyspark.sql import functions as F
        from datetime import datetime
        
        try:
            current_timestamp = datetime.now()
            
            # Add SCD columns to source
            source_with_scd = source_df.withColumn(
                "effective_date", F.lit(current_timestamp)
            ).withColumn(
                "end_date", F.lit(None).cast("timestamp")
            ).withColumn(
                "is_current", F.lit(True)
            )
            
            if DeltaTable.isDeltaTable(self.spark, target_path):
                delta_table = DeltaTable.forPath(self.spark, target_path)
                
                # Build merge condition
                merge_condition = " AND ".join([
                    f"target.{key} = source.{key}" for key in business_keys
                ]) + " AND target.is_current = true"
                
                # SCD Type 2 merge
                delta_table.alias("target").merge(
                    source_with_scd.alias("source"),
                    merge_condition
                ).whenMatchedUpdate(
                    condition="target.is_current = true",
                    set={
                        "end_date": F.lit(current_timestamp),
                        "is_current": F.lit(False)
                    }
                ).whenNotMatchedInsertAll().execute()
                
                # Insert new versions
                self.append_data(source_with_scd, target_path, partition_cols)
                
                logger.info(f"Updated SCD Type 2 table at {target_path}")
            else:
                # First time write
                writer = source_with_scd.write.format("delta").mode("overwrite")
                if partition_cols:
                    writer = writer.partitionBy(*partition_cols)
                writer.save(target_path)
                logger.info(f"Created SCD Type 2 table at {target_path}")
                
        except Exception as e:
            logger.error(f"Error creating SCD Type 2 table: {str(e)}")
            raise


def create_delta_table_if_not_exists(
    spark: SparkSession,
    table_path: str,
    schema_df: DataFrame,
    partition_cols: Optional[List[str]] = None
) -> None:
    """
    Create Delta table if it doesn't exist
    
    Args:
        spark: SparkSession
        table_path: S3 path for Delta table
        schema_df: DataFrame with schema (can be empty)
        partition_cols: Partition columns
    """
    if not DeltaTable.isDeltaTable(spark, table_path):
        writer = schema_df.write.format("delta").mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(table_path)
        logger.info(f"Created Delta table at {table_path}")
