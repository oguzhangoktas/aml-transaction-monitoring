"""
Unit Tests for AML Rules Engine
Tests business rules logic and risk scoring
"""
import unittest
from decimal import Decimal
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


class TestRulesEngine(unittest.TestCase):
    """Test suite for AML business rules"""
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session for tests"""
        cls.spark = SparkSession.builder \
            .appName("TestRulesEngine") \
            .master("local[2]") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
        
        cls.spark.sparkContext.setLogLevel("ERROR")
    
    @classmethod
    def tearDownClass(cls):
        """Tear down Spark session"""
        cls.spark.stop()
    
    def setUp(self):
        """Set up test data"""
        self.test_schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("transaction_type", StringType(), False),
            StructField("amount", DoubleType(), False),
            StructField("destination_country", StringType(), False),
            StructField("transaction_timestamp", TimestampType(), False),
            StructField("avg_transaction_amount", DoubleType(), True),
            StructField("is_pep", BooleanType(), True),
        ])
    
    def test_rule_1_large_cash_transaction(self):
        """Test Rule 1: Large cash transactions (> 10,000)"""
        test_data = [
            ("TXN001", "CUST001", "CASH_DEPOSIT", 15000.0, "DE", datetime.now(), 5000.0, False),
            ("TXN002", "CUST002", "CASH_DEPOSIT", 8000.0, "DE", datetime.now(), 5000.0, False),
            ("TXN003", "CUST003", "ATM_WITHDRAWAL", 12000.0, "DE", datetime.now(), 5000.0, False),
            ("TXN004", "CUST004", "ONLINE_TRANSFER", 15000.0, "DE", datetime.now(), 5000.0, False),
        ]
        
        df = self.spark.createDataFrame(test_data, self.test_schema)
        
        # Apply Rule 1
        df_with_rule = df.withColumn(
            "rule_1_triggered",
            (F.col("transaction_type").isin(['CASH_DEPOSIT', 'ATM_WITHDRAWAL'])) &
            (F.col("amount") > 10000)
        )
        
        results = df_with_rule.collect()
        
        # Assertions
        self.assertTrue(results[0]["rule_1_triggered"])  # TXN001: CASH_DEPOSIT 15,000
        self.assertFalse(results[1]["rule_1_triggered"])  # TXN002: CASH_DEPOSIT 8,000
        self.assertTrue(results[2]["rule_1_triggered"])  # TXN003: ATM_WITHDRAWAL 12,000
        self.assertFalse(results[3]["rule_1_triggered"])  # TXN004: ONLINE_TRANSFER (not cash)
    
    def test_rule_2_high_risk_country(self):
        """Test Rule 2: Transactions to high-risk countries with large amounts"""
        test_data = [
            ("TXN001", "CUST001", "WIRE_TRANSFER", 6000.0, "AE", datetime.now(), 5000.0, False),
            ("TXN002", "CUST002", "WIRE_TRANSFER", 4000.0, "AE", datetime.now(), 5000.0, False),
            ("TXN003", "CUST003", "WIRE_TRANSFER", 6000.0, "DE", datetime.now(), 5000.0, False),
            ("TXN004", "CUST004", "WIRE_TRANSFER", 6000.0, "RU", datetime.now(), 5000.0, False),
        ]
        
        df = self.spark.createDataFrame(test_data, self.test_schema)
        
        # Apply Rule 2
        df_with_rule = df \
            .withColumn(
                "is_high_risk_country",
                F.col("destination_country").isin(['AE', 'RU', 'CN', 'TR'])
            ) \
            .withColumn(
                "rule_2_triggered",
                F.col("is_high_risk_country") & (F.col("amount") > 5000)
            )
        
        results = df_with_rule.collect()
        
        # Assertions
        self.assertTrue(results[0]["rule_2_triggered"])  # AE, 6000
        self.assertFalse(results[1]["rule_2_triggered"])  # AE, 4000 (below threshold)
        self.assertFalse(results[2]["rule_2_triggered"])  # DE (not high-risk)
        self.assertTrue(results[3]["rule_2_triggered"])  # RU, 6000
    
    def test_rule_3_unusual_amount(self):
        """Test Rule 3: Amount significantly higher than customer average"""
        test_data = [
            ("TXN001", "CUST001", "WIRE_TRANSFER", 30000.0, "DE", datetime.now(), 5000.0, False),
            ("TXN002", "CUST002", "WIRE_TRANSFER", 15000.0, "DE", datetime.now(), 5000.0, False),
            ("TXN003", "CUST003", "WIRE_TRANSFER", 6000.0, "DE", datetime.now(), 5000.0, False),
            ("TXN004", "CUST004", "WIRE_TRANSFER", 6000.0, "DE", datetime.now(), None, False),
        ]
        
        df = self.spark.createDataFrame(test_data, self.test_schema)
        
        # Apply Rule 3
        df_with_rule = df \
            .withColumn(
                "amount_vs_avg_ratio",
                F.when(F.col("avg_transaction_amount") > 0,
                       F.col("amount") / F.col("avg_transaction_amount"))
                .otherwise(0)
            ) \
            .withColumn(
                "rule_3_triggered",
                F.col("amount_vs_avg_ratio") > 5
            )
        
        results = df_with_rule.collect()
        
        # Assertions
        self.assertTrue(results[0]["rule_3_triggered"])  # 30,000 / 5,000 = 6x
        self.assertFalse(results[1]["rule_3_triggered"])  # 15,000 / 5,000 = 3x
        self.assertFalse(results[2]["rule_3_triggered"])  # 6,000 / 5,000 = 1.2x
        self.assertFalse(results[3]["rule_3_triggered"])  # No average (handle null)
    
    def test_rule_4_pep_large_wire(self):
        """Test Rule 4: PEP with large wire transfers"""
        test_data = [
            ("TXN001", "CUST001", "WIRE_TRANSFER", 60000.0, "DE", datetime.now(), 5000.0, True),
            ("TXN002", "CUST002", "WIRE_TRANSFER", 60000.0, "DE", datetime.now(), 5000.0, False),
            ("TXN003", "CUST003", "WIRE_TRANSFER", 40000.0, "DE", datetime.now(), 5000.0, True),
            ("TXN004", "CUST004", "ONLINE_TRANSFER", 60000.0, "DE", datetime.now(), 5000.0, True),
        ]
        
        df = self.spark.createDataFrame(test_data, self.test_schema)
        
        # Apply Rule 4
        df_with_rule = df.withColumn(
            "rule_4_triggered",
            (F.col("is_pep") == True) &
            (F.col("transaction_type") == 'WIRE_TRANSFER') &
            (F.col("amount") > 50000)
        )
        
        results = df_with_rule.collect()
        
        # Assertions
        self.assertTrue(results[0]["rule_4_triggered"])  # PEP, WIRE, 60k
        self.assertFalse(results[1]["rule_4_triggered"])  # Not PEP
        self.assertFalse(results[2]["rule_4_triggered"])  # PEP but 40k (below threshold)
        self.assertFalse(results[3]["rule_4_triggered"])  # PEP, 60k but not WIRE
    
    def test_risk_score_calculation(self):
        """Test overall risk score calculation"""
        test_data = [
            ("TXN001", "CUST001", "WIRE_TRANSFER", 60000.0, "AE", datetime.now(), 5000.0, True),
        ]
        
        df = self.spark.createDataFrame(test_data, self.test_schema)
        
        # Apply all rules
        df_with_rules = df \
            .withColumn("rule_1_triggered", F.lit(False)) \
            .withColumn("rule_2_triggered", F.lit(True)) \
            .withColumn("rule_3_triggered", F.lit(True)) \
            .withColumn("rule_4_triggered", F.lit(True)) \
            .withColumn("rule_5_triggered", F.lit(False)) \
            .withColumn("rule_6_triggered", F.lit(False)) \
            .withColumn(
                "total_rules_triggered",
                F.col("rule_1_triggered").cast("int") +
                F.col("rule_2_triggered").cast("int") +
                F.col("rule_3_triggered").cast("int") +
                F.col("rule_4_triggered").cast("int") +
                F.col("rule_5_triggered").cast("int") +
                F.col("rule_6_triggered").cast("int")
            ) \
            .withColumn(
                "transaction_risk_score",
                (F.col("total_rules_triggered") * 0.2) + 0.3  # Assuming base customer risk of 0.3
            ) \
            .withColumn(
                "is_suspicious",
                F.when(F.col("transaction_risk_score") > 0.7, True).otherwise(False)
            )
        
        result = df_with_rules.collect()[0]
        
        # Assertions
        self.assertEqual(result["total_rules_triggered"], 3)
        self.assertAlmostEqual(result["transaction_risk_score"], 0.9, places=1)  # 3 * 0.2 + 0.3
        self.assertTrue(result["is_suspicious"])
    
    def test_alert_priority_classification(self):
        """Test alert priority based on risk score"""
        test_data = [
            ("TXN001", "CUST001", "WIRE_TRANSFER", 10000.0, "DE", datetime.now(), 5000.0, False, 0.95),
            ("TXN002", "CUST002", "WIRE_TRANSFER", 10000.0, "DE", datetime.now(), 5000.0, False, 0.75),
            ("TXN003", "CUST003", "WIRE_TRANSFER", 10000.0, "DE", datetime.now(), 5000.0, False, 0.55),
            ("TXN004", "CUST004", "WIRE_TRANSFER", 10000.0, "DE", datetime.now(), 5000.0, False, 0.35),
        ]
        
        schema_with_risk = StructType(self.test_schema.fields + [
            StructField("transaction_risk_score", DoubleType(), False)
        ])
        
        df = self.spark.createDataFrame(test_data, schema_with_risk)
        
        # Classify priority
        df_with_priority = df.withColumn(
            "alert_priority",
            F.when(F.col("transaction_risk_score") >= 0.9, "CRITICAL")
            .when(F.col("transaction_risk_score") >= 0.7, "HIGH")
            .when(F.col("transaction_risk_score") >= 0.5, "MEDIUM")
            .otherwise("LOW")
        )
        
        results = df_with_priority.collect()
        
        # Assertions
        self.assertEqual(results[0]["alert_priority"], "CRITICAL")
        self.assertEqual(results[1]["alert_priority"], "HIGH")
        self.assertEqual(results[2]["alert_priority"], "MEDIUM")
        self.assertEqual(results[3]["alert_priority"], "LOW")
    
    def test_unusual_time_detection(self):
        """Test detection of transactions at unusual times"""
        night_time = datetime(2024, 1, 1, 3, 0, 0)  # 3 AM
        day_time = datetime(2024, 1, 1, 14, 0, 0)  # 2 PM
        late_night = datetime(2024, 1, 1, 23, 30, 0)  # 11:30 PM
        
        test_data = [
            ("TXN001", "CUST001", "WIRE_TRANSFER", 10000.0, "DE", night_time, 5000.0, False),
            ("TXN002", "CUST002", "WIRE_TRANSFER", 10000.0, "DE", day_time, 5000.0, False),
            ("TXN003", "CUST003", "WIRE_TRANSFER", 10000.0, "DE", late_night, 5000.0, False),
        ]
        
        df = self.spark.createDataFrame(test_data, self.test_schema)
        
        # Detect unusual time
        df_with_time_flag = df.withColumn(
            "is_unusual_time",
            F.when(
                (F.hour("transaction_timestamp") < 6) | 
                (F.hour("transaction_timestamp") > 22),
                True
            ).otherwise(False)
        )
        
        results = df_with_time_flag.collect()
        
        # Assertions
        self.assertTrue(results[0]["is_unusual_time"])  # 3 AM
        self.assertFalse(results[1]["is_unusual_time"])  # 2 PM
        self.assertTrue(results[2]["is_unusual_time"])  # 11:30 PM


if __name__ == '__main__':
    unittest.main()
