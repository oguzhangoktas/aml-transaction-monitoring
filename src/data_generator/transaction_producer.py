"""
Transaction Data Generator and Kafka Producer
Generates realistic transaction data for testing the streaming pipeline
"""
import json
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List
from kafka import KafkaProducer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TransactionGenerator:
    """Generates realistic banking transaction data"""
    
    # Transaction types with risk weights
    TRANSACTION_TYPES = {
        'ATM_WITHDRAWAL': {'risk': 0.1, 'amount_range': (50, 500)},
        'POS_PURCHASE': {'risk': 0.05, 'amount_range': (10, 2000)},
        'ONLINE_TRANSFER': {'risk': 0.3, 'amount_range': (100, 50000)},
        'WIRE_TRANSFER': {'risk': 0.5, 'amount_range': (1000, 100000)},
        'CHECK_DEPOSIT': {'risk': 0.2, 'amount_range': (500, 10000)},
        'CASH_DEPOSIT': {'risk': 0.4, 'amount_range': (1000, 25000)},
    }
    
    # Countries with different risk levels
    COUNTRIES = {
        'DE': {'risk': 0.1, 'weight': 40},  # Germany - low risk, high volume
        'FR': {'risk': 0.1, 'weight': 15},
        'NL': {'risk': 0.1, 'weight': 10},
        'GB': {'risk': 0.1, 'weight': 10},
        'US': {'risk': 0.2, 'weight': 8},
        'CH': {'risk': 0.2, 'weight': 5},
        'AE': {'risk': 0.6, 'weight': 3},   # UAE - higher risk
        'RU': {'risk': 0.8, 'weight': 2},   # Russia - high risk
        'CN': {'risk': 0.5, 'weight': 5},
        'TR': {'risk': 0.4, 'weight': 2},
    }
    
    CURRENCIES = ['EUR', 'USD', 'GBP', 'CHF', 'AED', 'RUB', 'CNY', 'TRY']
    
    def __init__(self, num_customers: int = 10000):
        self.num_customers = num_customers
        self.customer_ids = [f"CUST{str(i).zfill(8)}" for i in range(1, num_customers + 1)]
        
    def generate_transaction(self, suspicious: bool = False) -> Dict:
        """
        Generate a single transaction
        
        Args:
            suspicious: If True, generate a suspicious transaction
            
        Returns:
            Transaction dictionary
        """
        customer_id = random.choice(self.customer_ids)
        transaction_type = random.choice(list(self.TRANSACTION_TYPES.keys()))
        
        # Select country with weighted probability
        countries = list(self.COUNTRIES.keys())
        weights = [self.COUNTRIES[c]['weight'] for c in countries]
        source_country = random.choices(countries, weights=weights)[0]
        dest_country = random.choices(countries, weights=weights)[0]
        
        # Generate amount
        amount_range = self.TRANSACTION_TYPES[transaction_type]['amount_range']
        if suspicious:
            # Suspicious transactions: larger amounts, high-risk countries
            amount = random.uniform(amount_range[1] * 0.8, amount_range[1] * 3)
            dest_country = random.choice(['AE', 'RU', 'CN'])  # High-risk countries
        else:
            amount = random.uniform(amount_range[0], amount_range[1])
        
        # Round amount to 2 decimals
        amount = round(amount, 2)
        
        # Currency (mostly EUR for German customers)
        currency = 'EUR' if source_country in ['DE', 'FR', 'NL'] and random.random() > 0.2 else random.choice(self.CURRENCIES)
        
        # Generate transaction
        transaction = {
            'transaction_id': f"TXN{int(time.time() * 1000000)}",
            'customer_id': customer_id,
            'transaction_type': transaction_type,
            'amount': amount,
            'currency': currency,
            'source_country': source_country,
            'destination_country': dest_country,
            'merchant_category': self._get_merchant_category(transaction_type),
            'timestamp': datetime.utcnow().isoformat(),
            'channel': random.choice(['ONLINE', 'BRANCH', 'ATM', 'MOBILE']),
            'device_id': f"DEV{random.randint(1000, 9999)}",
        }
        
        return transaction
    
    def _get_merchant_category(self, transaction_type: str) -> str:
        """Get merchant category based on transaction type"""
        categories = {
            'POS_PURCHASE': ['RETAIL', 'RESTAURANT', 'FUEL', 'GROCERY'],
            'ONLINE_TRANSFER': ['UTILITY', 'INVESTMENT', 'LOAN_PAYMENT', 'PERSONAL'],
            'ATM_WITHDRAWAL': ['CASH'],
            'WIRE_TRANSFER': ['BUSINESS', 'REAL_ESTATE', 'INVESTMENT', 'PERSONAL'],
            'CHECK_DEPOSIT': ['SALARY', 'BUSINESS', 'PERSONAL'],
            'CASH_DEPOSIT': ['BUSINESS', 'PERSONAL', 'SAVINGS'],
        }
        return random.choice(categories.get(transaction_type, ['OTHER']))
    
    def generate_batch(self, num_transactions: int, suspicious_rate: float = 0.02) -> List[Dict]:
        """
        Generate a batch of transactions
        
        Args:
            num_transactions: Number of transactions to generate
            suspicious_rate: Percentage of suspicious transactions (0.02 = 2%)
            
        Returns:
            List of transaction dictionaries
        """
        transactions = []
        num_suspicious = int(num_transactions * suspicious_rate)
        
        for i in range(num_transactions):
            is_suspicious = i < num_suspicious
            transaction = self.generate_transaction(suspicious=is_suspicious)
            transactions.append(transaction)
        
        # Shuffle to distribute suspicious transactions randomly
        random.shuffle(transactions)
        return transactions


class TransactionProducer:
    """Kafka producer for transaction data"""
    
    def __init__(
        self,
        bootstrap_servers: str = 'localhost:9092',
        topic: str = 'transactions'
    ):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda v: v.encode('utf-8') if v else None,
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1
        )
        self.generator = TransactionGenerator()
        logger.info(f"Kafka producer initialized for topic: {topic}")
    
    def send_transaction(self, transaction: Dict) -> None:
        """Send a single transaction to Kafka"""
        try:
            # Use customer_id as partition key for ordering
            key = transaction['customer_id']
            future = self.producer.send(
                self.topic,
                key=key,
                value=transaction
            )
            # Wait for send to complete
            future.get(timeout=10)
        except Exception as e:
            logger.error(f"Error sending transaction: {str(e)}")
            raise
    
    def produce_stream(
        self,
        transactions_per_second: int = 100,
        duration_seconds: int = 3600,
        suspicious_rate: float = 0.02
    ) -> None:
        """
        Produce continuous stream of transactions
        
        Args:
            transactions_per_second: Rate of transaction generation
            duration_seconds: How long to produce (default 1 hour)
            suspicious_rate: Percentage of suspicious transactions
        """
        logger.info(f"Starting transaction stream: {transactions_per_second} TPS for {duration_seconds}s")
        
        start_time = time.time()
        transaction_count = 0
        
        try:
            while time.time() - start_time < duration_seconds:
                batch_start = time.time()
                
                # Generate and send transactions for this second
                transactions = self.generator.generate_batch(
                    transactions_per_second,
                    suspicious_rate
                )
                
                for transaction in transactions:
                    self.send_transaction(transaction)
                    transaction_count += 1
                
                # Log progress every 10 seconds
                if transaction_count % (transactions_per_second * 10) == 0:
                    logger.info(f"Sent {transaction_count} transactions")
                
                # Sleep to maintain rate (adjust for processing time)
                elapsed = time.time() - batch_start
                sleep_time = max(0, 1.0 - elapsed)
                time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            logger.info("Stream interrupted by user")
        finally:
            self.producer.flush()
            self.producer.close()
            logger.info(f"Stream finished. Total transactions sent: {transaction_count}")
    
    def produce_batch_file(
        self,
        output_file: str,
        num_transactions: int = 10000,
        suspicious_rate: float = 0.02
    ) -> None:
        """
        Generate batch file for testing
        
        Args:
            output_file: Output JSON file path
            num_transactions: Number of transactions to generate
            suspicious_rate: Percentage of suspicious transactions
        """
        logger.info(f"Generating {num_transactions} transactions to {output_file}")
        
        transactions = self.generator.generate_batch(num_transactions, suspicious_rate)
        
        with open(output_file, 'w') as f:
            for transaction in transactions:
                f.write(json.dumps(transaction) + '\n')
        
        logger.info(f"Batch file created: {output_file}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate transaction data')
    parser.add_argument('--mode', choices=['stream', 'batch'], default='stream',
                        help='Generation mode: stream (Kafka) or batch (file)')
    parser.add_argument('--bootstrap-servers', default='localhost:9092',
                        help='Kafka bootstrap servers')
    parser.add_argument('--topic', default='transactions',
                        help='Kafka topic name')
    parser.add_argument('--tps', type=int, default=100,
                        help='Transactions per second (stream mode)')
    parser.add_argument('--duration', type=int, default=3600,
                        help='Duration in seconds (stream mode)')
    parser.add_argument('--output-file', default='data/sample/transactions.json',
                        help='Output file (batch mode)')
    parser.add_argument('--num-transactions', type=int, default=10000,
                        help='Number of transactions (batch mode)')
    parser.add_argument('--suspicious-rate', type=float, default=0.02,
                        help='Suspicious transaction rate (0.02 = 2%)')
    
    args = parser.parse_args()
    
    if args.mode == 'stream':
        producer = TransactionProducer(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic
        )
        producer.produce_stream(
            transactions_per_second=args.tps,
            duration_seconds=args.duration,
            suspicious_rate=args.suspicious_rate
        )
    else:
        producer = TransactionProducer()
        producer.produce_batch_file(
            output_file=args.output_file,
            num_transactions=args.num_transactions,
            suspicious_rate=args.suspicious_rate
        )
