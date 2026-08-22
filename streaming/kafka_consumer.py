import sys
import json
from pathlib import Path

# Add the parent folder (realtime-ecom) to Python's module path
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

import json
from kafka import KafkaConsumer
from data_quality.RapidTransactionDetector import RapidTransactionDetector

consumer = KafkaConsumer(
    'userevents',
    bootstrap_servers=['localhost:9092'],
    group_id = 'fraud-detector-group',
    value_deserializer = lambda x: json.loads(x.decode('utf-8'))
)

detector = RapidTransactionDetector(max_transactions=3, window_seconds=120)

for message in consumer:
    event = message.value
    try:
        detector.process_transaction(event)
    except ValueError as error:
        print(f"Alert {error}")